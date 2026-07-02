import assert from "node:assert/strict";
import { createSecretKey } from "node:crypto";
import { existsSync } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
  AuthStateStoreError,
  EncryptedAuthStateStore,
} from "../src/encrypted-auth-state-store.mjs";

const AUTHORITY_ID = "authority-test-001";
const PAYLOAD = JSON.stringify({
  auth_mode: "chatgpt",
  tokens: {
    access_token: "access-secret-sentinel",
    id_token: "id-secret-sentinel",
    refresh_token: "refresh-secret-sentinel",
  },
});
const TRUSTED_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectExtendedAcl: async () => false,
});

function createKeyProvider() {
  const keys = new Map([
    ["key-001", createSecretKey(Buffer.alloc(32, 1))],
    ["key-002", createSecretKey(Buffer.alloc(32, 2))],
  ]);
  let activeKeyId = "key-001";
  return {
    activate(keyId) {
      activeKeyId = keyId;
    },
    delete(keyId) {
      keys.delete(keyId);
    },
    async activeKey() {
      return { keyId: activeKeyId, key: keys.get(activeKeyId) };
    },
    async getKey(keyId) {
      return keys.get(keyId);
    },
  };
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

async function createStoreFixture(t, options = {}) {
  const root = await mkdtemp(join(tmpdir(), "encrypted-auth-state-store-"));
  const directory = join(root, "authority");
  await mkdir(directory, { mode: 0o700 });
  await chmod(directory, 0o700);
  t.after(() => rm(root, { force: true, recursive: true }));
  const keyProvider = options.keyProvider ?? createKeyProvider();
  const store = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory,
    keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
    ...options,
  });
  return { directory, keyProvider, root, store };
}

function assertStoreError(code, extra = () => true) {
  return (error) =>
    error instanceof AuthStateStoreError && error.code === code && extra(error);
}

function ancestorCount(path) {
  let count = 0;
  let ancestor = dirname(path);
  while (true) {
    count += 1;
    if (ancestor === dirname(ancestor)) return count;
    ancestor = dirname(ancestor);
  }
}

test("encrypted store persists exact payload with monotonic CAS and idempotent replay", async (t) => {
  const { directory, store } = await createStoreFixture(t);
  assert.equal(await store.read(), null);
  assert.equal(store.coordinationKey, JSON.stringify([store.directory, AUTHORITY_ID]));

  const first = await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  assert.deepEqual(Object.keys(first), ["generation", "commitId", "keyId", "replayed"]);
  assert.deepEqual(
    { ...first },
    {
      generation: "1",
      commitId: "commit-001",
      keyId: "key-001",
      replayed: false,
    },
  );
  assert.equal(first.payload, PAYLOAD);
  assert.equal(Object.getOwnPropertyDescriptor(first, "payload").enumerable, false);
  assert(Object.isFrozen(first));

  const rawEnvelope = await readFile(join(directory, "auth-state.enc"), "utf8");
  assert(rawEnvelope.endsWith("\n"));
  for (const secret of ["access-secret-sentinel", "id-secret-sentinel", "refresh-secret-sentinel"]) {
    assert.equal(rawEnvelope.includes(secret), false);
  }

  const replay = await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  assert.equal(replay.generation, "1");
  assert.equal(replay.replayed, true);
  await assert.rejects(
    store.compareAndSwap({
      expectedGeneration: "1",
      commitId: "commit-001",
      payload: PAYLOAD,
    }),
    assertStoreError("generation_conflict", (error) => error.retryable === true),
  );
  await assert.rejects(
    store.compareAndSwap({
      expectedGeneration: "1",
      commitId: "commit-001",
      payload: `${PAYLOAD}\n`,
    }),
    assertStoreError("commit_id_conflict"),
  );
  await assert.rejects(
    store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-002",
      payload: PAYLOAD,
    }),
    assertStoreError("generation_conflict", (error) => error.retryable === true),
  );

  const secondPayload = `${PAYLOAD} `;
  const second = await store.compareAndSwap({
    expectedGeneration: "1",
    commitId: "commit-002",
    payload: secondPayload,
  });
  assert.equal(second.generation, "2");
  assert.equal((await store.read()).payload, secondPayload);
});

test("oversized encrypted envelopes fail before replacing canonical state", async (t) => {
  let renameCalls = 0;
  const { directory, store } = await createStoreFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {},
      async release() {},
      async renameWhileHeld(source, destination) {
        renameCalls += 1;
        await rename(source, destination);
      },
    }),
  });
  const first = await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-before-oversized-envelope",
    payload: PAYLOAD,
  });
  assert.equal(first.generation, "1");
  renameCalls = 0;

  await assert.rejects(
    store.compareAndSwap({
      expectedGeneration: "1",
      commitId: "commit-oversized-envelope",
      payload: "\0".repeat(1024 * 1024),
    }),
    assertStoreError("invalid_payload", (error) => error.commitState === undefined),
  );

  assert.equal(renameCalls, 0);
  assert.equal(
    (await readdir(directory)).some((entry) => entry.startsWith(".auth-state.enc.next-")),
    false,
  );
  const current = await store.read();
  assert.equal(current.generation, "1");
  assert.equal(current.payload, PAYLOAD);
});

test("maximum plain ASCII payload remains writable", async (t) => {
  const { store } = await createStoreFixture(t);
  const payload = "a".repeat(1024 * 1024);

  const committed = await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-maximum-ascii-payload",
    payload,
  });

  assert.equal(committed.generation, "1");
  assert.equal((await store.read()).payload, payload);
});

test("ACL policy evidence is reused only for unchanged metadata in one transaction", async (t) => {
  const ancestorAclInspections = new Map();
  let extendedAclInspections = 0;
  let metadataReads = 0;
  const fixture = await createStoreFixture(t, {
    inspectAncestorAcl: async (path) => {
      ancestorAclInspections.set(path, (ancestorAclInspections.get(path) ?? 0) + 1);
      return false;
    },
    inspectExtendedAcl: async () => {
      extendedAclInspections += 1;
      return false;
    },
    readPathMetadata: async (...args) => {
      metadataReads += 1;
      return lstat(...args);
    },
  });
  const ancestors = ancestorCount(fixture.store.directory);
  const stableAncestor = dirname(fixture.store.directory);

  assert.equal(await fixture.store.read(), null);
  assert.equal(extendedAclInspections, 1);
  assert.equal(ancestorAclInspections.get(stableAncestor), 3);
  assert.equal(ancestorAclInspections.size, ancestors);
  assert(metadataReads > ancestors);

  assert.equal(await fixture.store.read(), null);
  assert.equal(extendedAclInspections, 2);
  assert.equal(ancestorAclInspections.get(stableAncestor), 6);

  const beforeMutationExtended = extendedAclInspections;
  const beforeMutationRootInspections = ancestorAclInspections.get(stableAncestor);
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-acl-cache-invalidation",
    payload: PAYLOAD,
  });
  assert(extendedAclInspections > beforeMutationExtended);
  assert(extendedAclInspections - beforeMutationExtended <= 3);
  assert.equal(
    ancestorAclInspections.get(stableAncestor) - beforeMutationRootInspections,
    5,
  );
});

test("ACL policy changes are rechecked at commit boundaries", async (t) => {
  await t.test("authority ACL becomes unsafe before rename", async (t) => {
    let unsafe = false;
    const fixture = await createStoreFixture(t, {
      faults: {
        afterTempSync() {
          unsafe = true;
        },
      },
      inspectExtendedAcl: async () => unsafe,
    });

    await assert.rejects(
      fixture.store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-unsafe-authority-acl",
        payload: PAYLOAD,
      }),
      assertStoreError("invalid_store_directory"),
    );
  });

  await t.test("ancestor ACL becomes unsafe after rename", async (t) => {
    let unsafe = false;
    const fixture = await createStoreFixture(t, {
      faults: {
        afterRename() {
          unsafe = true;
        },
      },
      inspectAncestorAcl: async () => unsafe,
    });

    await assert.rejects(
      fixture.store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-unsafe-ancestor-acl",
        payload: PAYLOAD,
      }),
      assertStoreError(
        "commit_outcome_uncertain",
        (error) => error.commitState === "uncertain" && error.retryable === false,
      ),
    );
  });
});

test("coordination keys encode directory and authority without delimiter collisions", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "auth-store-coordination-key-"));
  const firstDirectory = join(root, "a::b");
  const secondDirectory = join(root, "a");
  await mkdir(firstDirectory, { mode: 0o700 });
  await mkdir(secondDirectory, { mode: 0o700 });
  t.after(() => rm(root, { force: true, recursive: true }));
  const keyProvider = createKeyProvider();
  const first = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: "c",
    directory: firstDirectory,
    keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });
  const second = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: "b::c",
    directory: secondDirectory,
    keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });

  assert.notEqual(first.coordinationKey, second.coordinationKey);
  assert.deepEqual(JSON.parse(first.coordinationKey), [first.directory, "c"]);
  assert.deepEqual(JSON.parse(second.coordinationKey), [second.directory, "b::c"]);
});

test("coordination keys canonicalize filesystem aliases to one authority", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "auth-store-coordination-alias-"));
  const directory = join(root, "authority");
  const alias = join(root, "authority-alias");
  await mkdir(directory, { mode: 0o700 });
  await symlink(directory, alias, "dir");
  t.after(() => rm(root, { force: true, recursive: true }));
  const keyProvider = createKeyProvider();
  const direct = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory,
    keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });
  const throughAlias = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory: alias,
    keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });

  assert.equal(throughAlias.coordinationKey, direct.coordinationKey);
  assert.equal(throughAlias.directory, direct.directory);
});

test("key rotation re-encrypts under the active key and preserves payload", async (t) => {
  const { directory, keyProvider, store } = await createStoreFixture(t);
  await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  const before = await readFile(join(directory, "auth-state.enc"), "utf8");
  keyProvider.activate("key-002");

  await assert.rejects(
    store.rotateEncryption({
      expectedGeneration: "1",
      commitId: "commit-001",
    }),
    assertStoreError("commit_id_conflict"),
  );

  const rotated = await store.rotateEncryption({
    expectedGeneration: "1",
    commitId: "rotation-001",
  });
  assert.deepEqual(
    { ...rotated },
    {
      generation: "2",
      commitId: "rotation-001",
      keyId: "key-002",
      replayed: false,
    },
  );
  assert.equal(rotated.payload, PAYLOAD);
  const after = await readFile(join(directory, "auth-state.enc"), "utf8");
  assert.notEqual(after, before);
  assert.equal(JSON.parse(after).keyId, "key-002");
  keyProvider.delete("key-001");
  assert.equal((await store.read()).payload, PAYLOAD);

  const replay = await store.rotateEncryption({
    expectedGeneration: "1",
    commitId: "rotation-001",
  });
  assert.equal(replay.generation, "2");
  assert.equal(replay.replayed, true);
  await assert.rejects(
    store.rotateEncryption({
      expectedGeneration: "2",
      commitId: "rotation-001",
    }),
    assertStoreError("generation_conflict", (error) => error.retryable === true),
  );
  await assert.rejects(
    store.compareAndSwap({
      expectedGeneration: "2",
      commitId: "rotation-001",
      payload: PAYLOAD,
    }),
    assertStoreError("commit_id_conflict"),
  );
});

test("authenticated envelope rejects tampering, wrong authority, and noncanonical JSON", async (t) => {
  const { directory, keyProvider, store } = await createStoreFixture(t);
  await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  const path = join(directory, "auth-state.enc");
  const original = await readFile(path, "utf8");
  const envelope = JSON.parse(original);
  envelope.ciphertext = `${envelope.ciphertext.slice(0, -1)}${
    envelope.ciphertext.endsWith("A") ? "B" : "A"
  }`;
  await writeFile(path, `${JSON.stringify(envelope)}\n`, { mode: 0o600 });
  await assert.rejects(store.read(), assertStoreError("auth_state_integrity_failed"));

  await writeFile(path, ` ${original}`, { mode: 0o600 });
  await assert.rejects(store.read(), assertStoreError("invalid_auth_state"));
  await writeFile(path, original, { mode: 0o600 });

  const otherAuthority = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: "authority-test-002",
    directory,
    keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });
  await assert.rejects(otherAuthority.read(), assertStoreError("authority_binding_mismatch"));
});

test("canonical symbolic link is invalid state rather than a retryable I/O failure", async (t) => {
  const fixture = await createStoreFixture(t);
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-before-canonical-symlink",
    payload: PAYLOAD,
  });
  const canonicalPath = join(fixture.directory, "auth-state.enc");
  const targetPath = join(fixture.directory, "auth-state.real");
  await rename(canonicalPath, targetPath);
  await symlink("auth-state.real", canonicalPath);

  await assert.rejects(fixture.store.read(), assertStoreError("invalid_auth_state"));
});

test("missing and invalid key material fail without exposing payloads", async (t) => {
  const keyProvider = createKeyProvider();
  const { store } = await createStoreFixture(t, { keyProvider });
  await store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  keyProvider.delete("key-001");
  await assert.rejects(
    store.read(),
    (error) => {
      assertStoreError("key_unavailable")(error);
      assert.equal(error.message.includes("secret-sentinel"), false);
      assert.equal(JSON.stringify(error).includes("secret-sentinel"), false);
      return true;
    },
  );

  const invalidProvider = {
    async activeKey() {
      return { keyId: "key-invalid", key: createSecretKey(Buffer.alloc(16)) };
    },
    async getKey() {
      return createSecretKey(Buffer.alloc(16));
    },
  };
  const invalid = await createStoreFixture(t, { keyProvider: invalidProvider });
  await assert.rejects(
    invalid.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-invalid",
      payload: PAYLOAD,
    }),
    assertStoreError("invalid_key_material"),
  );
});

test("pre-rename crash leaves a recovery candidate and preserves canonical state", async (t) => {
  const fixture = await createStoreFixture(t);
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  let failOnce = true;
  const crashing = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory: fixture.directory,
    faults: {
      afterTempSync() {
        if (failOnce) {
          failOnce = false;
          throw new Error("synthetic pre-rename crash");
        }
      },
    },
    keyProvider: fixture.keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });
  await assert.rejects(
    crashing.compareAndSwap({
      expectedGeneration: "1",
      commitId: "commit-002",
      payload: `${PAYLOAD} `,
    }),
    assertStoreError(
      "auth_state_io_failed",
      (error) => error.commitState === "not-committed",
    ),
  );
  const entries = await readdir(fixture.directory);
  const candidates = entries.filter((entry) => entry.startsWith(".auth-state.enc.next-"));
  assert.equal(candidates.length, 1);
  await assert.rejects(fixture.store.read(), assertStoreError("recovery_required"));
  await rm(join(fixture.directory, candidates[0]), { force: true });
  const current = await fixture.store.read();
  assert.equal(current.generation, "1");
  assert.equal(current.payload, PAYLOAD);
});

test("post-rename failure is uncertain and a clean reread observes the committed generation", async (t) => {
  const fixture = await createStoreFixture(t);
  let failOnce = true;
  const uncertain = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory: fixture.directory,
    faults: {
      afterRename() {
        if (failOnce) {
          failOnce = false;
          throw new Error("synthetic lost rename acknowledgement");
        }
      },
    },
    keyProvider: fixture.keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });
  await assert.rejects(
    uncertain.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-001",
      payload: PAYLOAD,
    }),
    assertStoreError(
      "commit_outcome_uncertain",
      (error) => error.commitState === "uncertain" && error.retryable === false,
    ),
  );
  const current = await fixture.store.read();
  assert.equal(current.generation, "1");
  assert.equal(current.commitId, "commit-001");
  assert.equal(current.payload, PAYLOAD);
});

test("visible CAS replay and reads require a successful directory sync proof", async (t) => {
  let allowDirectorySync = true;
  let syncCalls = 0;
  const fixture = await createStoreFixture(t, {
    async syncDirectory({ handle }) {
      syncCalls += 1;
      if (!allowDirectorySync) throw new Error("synthetic pre-sync failure");
      await handle.sync();
    },
  });
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  allowDirectorySync = false;
  const request = {
    expectedGeneration: "1",
    commitId: "commit-unproven-directory-sync",
    payload: `${PAYLOAD} `,
  };

  await assert.rejects(
    fixture.store.compareAndSwap(request),
    assertStoreError(
      "commit_outcome_uncertain",
      (error) => error.commitState === "uncertain" && error.retryable === false,
    ),
  );
  const callsAfterRename = syncCalls;
  await assert.rejects(
    fixture.store.compareAndSwap(request),
    assertStoreError("auth_state_io_failed"),
  );
  assert(syncCalls > callsAfterRename);
  await assert.rejects(fixture.store.read(), assertStoreError("auth_state_io_failed"));

  allowDirectorySync = true;
  const replay = await fixture.store.compareAndSwap(request);
  assert.equal(replay.generation, "2");
  assert.equal(replay.replayed, true);
  const current = await fixture.store.read();
  assert.equal(current.generation, "2");
  assert.equal(current.commitId, request.commitId);
  assert.equal(current.payload, request.payload);
});

test("post-rename readback key failure is normalized to an uncertain commit", async (t) => {
  const backingKeyProvider = createKeyProvider();
  let rejectKeyReads = false;
  const keyProvider = {
    ...backingKeyProvider,
    async getKey(keyId) {
      if (rejectKeyReads) throw new Error("synthetic key service outage");
      return backingKeyProvider.getKey(keyId);
    },
  };
  const fixture = await createStoreFixture(t, {
    faults: {
      afterRename() {
        rejectKeyReads = true;
      },
    },
    keyProvider,
  });

  await assert.rejects(
    fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-key-readback",
      payload: PAYLOAD,
    }),
    assertStoreError(
      "commit_outcome_uncertain",
      (error) => error.commitState === "uncertain" && error.retryable === false,
    ),
  );
  rejectKeyReads = false;
  const current = await fixture.store.read();
  assert.equal(current.generation, "1");
  assert.equal(current.commitId, "commit-key-readback");
});

test("rename provider failure after promotion is treated as uncertain", async (t) => {
  const fixture = await createStoreFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {},
      async release() {},
      async renameWhileHeld(source, destination) {
        await rename(source, destination);
        throw new Error("synthetic lost provider acknowledgement");
      },
    }),
  });

  await assert.rejects(
    fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-provider-rename",
      payload: PAYLOAD,
    }),
    assertStoreError(
      "commit_outcome_uncertain",
      (error) => error.commitState === "uncertain" && error.retryable === false,
    ),
  );
  const current = await new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory: fixture.directory,
    keyProvider: fixture.keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  }).read();
  assert.equal(current.generation, "1");
  assert.equal(current.commitId, "commit-provider-rename");
});

test("rename provider can explicitly prove that promotion did not occur", async (t) => {
  const fixture = await createStoreFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {},
      async release() {},
      async renameWhileHeld() {
        const error = new Error("synthetic rejected promotion");
        error.renameOutcome = "not-committed";
        throw error;
      },
    }),
  });

  await assert.rejects(
    fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-provider-rejected",
      payload: PAYLOAD,
    }),
    assertStoreError(
      "auth_state_io_failed",
      (error) => error.commitState === "not-committed" && error.retryable === false,
    ),
  );
});

test("pre-rename lock replacement is recovery-required and definitely not committed", async (t) => {
  const fixture = await createStoreFixture(t);
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  const canonicalPath = join(fixture.directory, "auth-state.enc");
  const before = await readFile(canonicalPath, "utf8");
  const replacedLockStore = new EncryptedAuthStateStore({
    acquireLock: async () => ({
      async assertHeld() {},
      async release() {},
      async renameWhileHeld() {
        const error = Object.assign(new Error("synthetic lock replacement"), {
          code: "lock_replaced",
          renameOutcome: "not-committed",
        });
        throw error;
      },
    }),
    authorityId: AUTHORITY_ID,
    directory: fixture.directory,
    keyProvider: fixture.keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });

  await assert.rejects(
    replacedLockStore.compareAndSwap({
      expectedGeneration: "1",
      commitId: "commit-lock-replaced-before-rename",
      payload: `${PAYLOAD} `,
    }),
    assertStoreError(
      "recovery_required",
      (error) => error.commitState === "not-committed" && error.retryable === false,
    ),
  );
  assert.equal(await readFile(canonicalPath, "utf8"), before);
  const entries = await readdir(fixture.directory);
  assert.equal(entries.some((entry) => entry.startsWith(".auth-state.enc.next-")), true);
});

test("hostile external error accessors are normalized without reading them", async (t) => {
  const secret = "hostile-store-error-secret";
  const hostile = {};
  for (const key of ["code", "renameOutcome"]) {
    Object.defineProperty(hostile, key, {
      get() {
        throw new Error(`${secret}-${key}`);
      },
    });
  }
  const fixture = await createStoreFixture(t, {
    faults: {
      afterTempSync() {
        throw hostile;
      },
    },
  });

  await assert.rejects(
    fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-hostile-error",
      payload: PAYLOAD,
    }),
    (error) => {
      assertStoreError("auth_state_io_failed")(error);
      assert.equal(error.message.includes(secret), false);
      assert.equal(JSON.stringify(error).includes(secret), false);
      return true;
    },
  );
});

test("unsafe advisory lock state requires operator recovery", async (t) => {
  const fixture = await createStoreFixture(t, {
    acquireLock: async () => {
      throw Object.assign(new Error("private unsafe lock detail"), {
        code: "unsafe_lock_file",
      });
    },
  });

  await assert.rejects(
    fixture.store.read(),
    assertStoreError(
      "recovery_required",
      (error) => error.retryable === false && error.commitState === "not-committed",
    ),
  );
});

test("directory replacement before rename fails closed", async (t) => {
  let fixture;
  fixture = await createStoreFixture(t, {
    faults: {
      async afterTempSync() {
        await rename(fixture.directory, `${fixture.directory}.displaced`);
        await mkdir(fixture.directory, { mode: 0o700 });
        await chmod(fixture.directory, 0o700);
      },
    },
  });

  await assert.rejects(
    fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-directory-swap",
      payload: PAYLOAD,
    }),
    assertStoreError("invalid_store_directory"),
  );
  assert.throws(
    () =>
      new EncryptedAuthStateStore({
        acquireLock: simpleLockProvider(),
        authorityId: AUTHORITY_ID,
        directory: fixture.directory,
        keyProvider: fixture.keyProvider,
        ...TRUSTED_ACL_INSPECTORS,
      }),
    assertStoreError("invalid_store_directory"),
  );
});

test("directory identity is pinned across transactions", async (t) => {
  let lockAcquisitions = 0;
  const fixture = await createStoreFixture(t, {
    acquireLock: async (...args) => {
      lockAcquisitions += 1;
      return simpleLockProvider()(...args);
    },
  });
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  const acquisitionsBeforeReplacement = lockAcquisitions;
  const displaced = `${fixture.directory}.displaced`;
  await rename(fixture.directory, displaced);
  await mkdir(fixture.directory, { mode: 0o700 });
  await chmod(fixture.directory, 0o700);

  await assert.rejects(fixture.store.read(), assertStoreError("invalid_store_directory"));
  assert.equal(lockAcquisitions, acquisitionsBeforeReplacement);
  assert.throws(
    () =>
      new EncryptedAuthStateStore({
        acquireLock: simpleLockProvider(),
        authorityId: AUTHORITY_ID,
        directory: fixture.directory,
        keyProvider: fixture.keyProvider,
        ...TRUSTED_ACL_INSPECTORS,
      }),
    assertStoreError("invalid_store_directory"),
  );
});

test("constructor-time directory identity protects the first transaction", async (t) => {
  let lockAcquisitions = 0;
  const fixture = await createStoreFixture(t, {
    acquireLock: async (...args) => {
      lockAcquisitions += 1;
      return simpleLockProvider()(...args);
    },
  });
  const displaced = `${fixture.directory}.displaced`;
  await rename(fixture.directory, displaced);
  await mkdir(fixture.directory, { mode: 0o700 });
  await chmod(fixture.directory, 0o700);

  await assert.rejects(fixture.store.read(), assertStoreError("invalid_store_directory"));
  assert.equal(lockAcquisitions, 0);
});

test("runtime canonical-path drift is rejected before lock acquisition", async (t) => {
  let lockAcquisitions = 0;
  const fixture = await createStoreFixture(t, {
    acquireLock: async (...args) => {
      lockAcquisitions += 1;
      return simpleLockProvider()(...args);
    },
  });
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-001",
    payload: PAYLOAD,
  });
  const acquisitionsBeforeDrift = lockAcquisitions;
  const displaced = `${fixture.directory}.displaced`;
  await rename(fixture.directory, displaced);
  await symlink(displaced, fixture.directory);

  await assert.rejects(fixture.store.read(), assertStoreError("invalid_store_directory"));
  assert.equal(lockAcquisitions, acquisitionsBeforeDrift);
});

test("unsafe directory or ancestor ACL is rejected before acquiring the lock", async (t) => {
  let lockAcquired = false;
  const fixture = await createStoreFixture(t, {
    acquireLock: async () => {
      lockAcquired = true;
      return simpleLockProvider()();
    },
    inspectExtendedAcl: async () => true,
  });

  await assert.rejects(fixture.store.read(), assertStoreError("invalid_store_directory"));
  assert.equal(lockAcquired, false);

  const unsafeAncestor = await createStoreFixture(t, {
    inspectAncestorAcl: async () => true,
  });
  await assert.rejects(
    unsafeAncestor.store.read(),
    assertStoreError("invalid_store_directory"),
  );
});

test("read transaction release failure is not reported as a committed mutation", async (t) => {
  const fixture = await createStoreFixture(t);
  await fixture.store.compareAndSwap({
    expectedGeneration: "0",
    commitId: "commit-before-read-release",
    payload: PAYLOAD,
  });
  const releaseFailureStore = new EncryptedAuthStateStore({
    acquireLock: async () => ({
      async assertHeld() {},
      async release() {
        throw new Error("synthetic release failure");
      },
      async renameWhileHeld(source, destination) {
        await rename(source, destination);
      },
    }),
    authorityId: AUTHORITY_ID,
    directory: fixture.directory,
    keyProvider: fixture.keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });

  await assert.rejects(
    releaseFailureStore.read(),
    assertStoreError(
      "lock_release_failed",
      (error) => error.commitState === "not-committed" && error.retryable === false,
    ),
  );
});

test("lock release failure overrides primary errors without losing commit state", async (t) => {
  await t.test("retryable generation conflict", async (t) => {
    const fixture = await createStoreFixture(t);
    await fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-before-release-conflict",
      payload: PAYLOAD,
    });
    const store = new EncryptedAuthStateStore({
      acquireLock: async () => ({
        async assertHeld() {},
        async release() {
          throw new Error("synthetic release failure");
        },
        async renameWhileHeld(source, destination) {
          await rename(source, destination);
        },
      }),
      authorityId: AUTHORITY_ID,
      directory: fixture.directory,
      keyProvider: fixture.keyProvider,
      ...TRUSTED_ACL_INSPECTORS,
    });

    await assert.rejects(
      store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-release-after-conflict",
        payload: PAYLOAD,
      }),
      assertStoreError(
        "lock_release_failed",
        (error) => error.commitState === "not-committed" && error.retryable === false,
      ),
    );
  });

  await t.test("uncertain rename acknowledgement", async (t) => {
    const fixture = await createStoreFixture(t);
    const store = new EncryptedAuthStateStore({
      acquireLock: async () => ({
        async assertHeld() {},
        async release() {
          throw new Error("synthetic release failure");
        },
        async renameWhileHeld(source, destination) {
          await rename(source, destination);
          throw new Error("synthetic lost rename acknowledgement");
        },
      }),
      authorityId: AUTHORITY_ID,
      directory: fixture.directory,
      keyProvider: fixture.keyProvider,
      ...TRUSTED_ACL_INSPECTORS,
    });

    await assert.rejects(
      store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-release-after-uncertain-rename",
        payload: PAYLOAD,
      }),
      assertStoreError(
        "lock_release_failed",
        (error) => error.commitState === "uncertain" && error.retryable === false,
      ),
    );
  });

  await t.test("hostile inherited commit-state getter", async (t) => {
    class HostileCommitStateError extends AuthStateStoreError {
      get commitState() {
        throw new Error("hostile commit-state getter must not run");
      }
    }
    const fixture = await createStoreFixture(t, {
      acquireLock: async () => ({
        async assertHeld() {},
        async release() {
          throw new Error("synthetic release failure");
        },
        async renameWhileHeld(source, destination) {
          await rename(source, destination);
        },
      }),
      faults: {
        afterTempSync() {
          throw new HostileCommitStateError("synthetic_primary", "synthetic primary failure");
        },
      },
    });

    await assert.rejects(
      fixture.store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-hostile-primary-state",
        payload: PAYLOAD,
      }),
      assertStoreError(
        "lock_release_failed",
        (error) => error.commitState === "not-committed" && error.retryable === false,
      ),
    );
  });

  await t.test("invalid primary commit-state value", async (t) => {
    const fixture = await createStoreFixture(t, {
      acquireLock: async () => ({
        async assertHeld() {},
        async release() {
          throw new Error("synthetic release failure");
        },
        async renameWhileHeld(source, destination) {
          await rename(source, destination);
        },
      }),
      faults: {
        afterTempSync() {
          throw new AuthStateStoreError("synthetic_primary", "synthetic primary failure", {
            commitState: "invalid-state",
          });
        },
      },
    });

    await assert.rejects(
      fixture.store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-invalid-primary-state",
        payload: PAYLOAD,
      }),
      assertStoreError(
        "lock_release_failed",
        (error) => error.commitState === "not-committed" && error.retryable === false,
      ),
    );
  });

  await t.test("untrusted committed primary state", async (t) => {
    const fixture = await createStoreFixture(t, {
      acquireLock: async () => ({
        async assertHeld() {},
        async release() {
          throw new Error("synthetic release failure");
        },
        async renameWhileHeld(source, destination) {
          await rename(source, destination);
        },
      }),
      faults: {
        afterTempSync() {
          throw new AuthStateStoreError("synthetic_primary", "synthetic primary failure", {
            commitState: "committed",
          });
        },
      },
    });

    await assert.rejects(
      fixture.store.compareAndSwap({
        expectedGeneration: "0",
        commitId: "commit-untrusted-committed-state",
        payload: PAYLOAD,
      }),
      assertStoreError(
        "lock_release_failed",
        (error) => error.commitState === "not-committed" && error.retryable === false,
      ),
    );
  });
});

test("process-local coordination serializes competing CAS calls across store instances", async (t) => {
  const fixture = await createStoreFixture(t);
  const secondStore = new EncryptedAuthStateStore({
    acquireLock: simpleLockProvider(),
    authorityId: AUTHORITY_ID,
    directory: fixture.directory,
    keyProvider: fixture.keyProvider,
    ...TRUSTED_ACL_INSPECTORS,
  });
  assert.equal(secondStore.coordinationKey, fixture.store.coordinationKey);
  const outcomes = await Promise.allSettled([
    fixture.store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-a",
      payload: `${PAYLOAD}a`,
    }),
    secondStore.compareAndSwap({
      expectedGeneration: "0",
      commitId: "commit-b",
      payload: `${PAYLOAD}b`,
    }),
  ]);
  assert.equal(outcomes.filter((outcome) => outcome.status === "fulfilled").length, 1);
  const rejection = outcomes.find((outcome) => outcome.status === "rejected");
  assertStoreError("generation_conflict")(rejection.reason);
  assert.equal((await fixture.store.read()).generation, "1");
});

const defaultLockAvailable =
  (process.platform === "darwin" && existsSync("/usr/bin/lockf")) ||
  (process.platform === "linux" && existsSync("/usr/bin/flock") && existsSync("/proc/self/fd"));

test(
  "default advisory lock supports a smoke round-trip",
  { skip: !defaultLockAvailable },
  async (t) => {
    const root = await mkdtemp(join(tmpdir(), "encrypted-auth-state-default-lock-"));
    const directory = join(root, "authority");
    await mkdir(directory, { mode: 0o700 });
    await chmod(directory, 0o700);
    t.after(() => rm(root, { force: true, recursive: true }));
    const store = new EncryptedAuthStateStore({
      authorityId: AUTHORITY_ID,
      directory,
      keyProvider: createKeyProvider(),
    });
    const committed = await store.compareAndSwap({
      expectedGeneration: "0",
      commitId: "default-lock-001",
      payload: PAYLOAD,
    });
    assert.equal(committed.generation, "1");
    assert.equal((await store.read()).payload, PAYLOAD);
  },
);
