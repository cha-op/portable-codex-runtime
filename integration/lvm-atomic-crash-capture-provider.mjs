import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  open,
  readFile,
  rename,
  rm,
  rmdir,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import { Pool } from "pg";

import {
  createLvmAtomicCrashCaptureDriver,
  createLvmAtomicCrashCaptureProvider,
} from "../src/lvm-atomic-crash-capture-provider.mjs";
import {
  createPostgresAtomicCrashCaptureCatalogue,
} from "../src/postgres-atomic-crash-capture-catalogue.mjs";
import {
  PostgresSerializableStore,
} from "../src/postgres-serializable-store.mjs";
import {
  capturePreparedAtomicCrashCheckpoint,
  prepareAtomicCrashCapture,
  verifyCommittedAtomicCrashCapture,
} from "../src/session-crash-capture-core.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
} from "../src/stopped-writer-capability.mjs";

const execFileAsync = promisify(execFile);
const ENABLED = process.env.LVM_ATOMIC_CRASH_CAPTURE_INTEGRATION === "1";
const DATABASE_URL =
  process.env.ATOMIC_CRASH_CAPTURE_DATABASE_URL ??
  process.env.SESSION_AUTHORITY_DATABASE_URL ??
  null;
const ROOT_PREFIX = "portable-codex-runtime-lvm-atomic";
const COMMAND_TIMEOUT_MILLISECONDS = 30_000;

function integrationRoot(value) {
  const candidate = resolve(value);
  const name = basename(candidate);
  if (
    value !== candidate ||
    dirname(candidate) !== "/var/tmp" ||
    (name !== ROOT_PREFIX && !name.startsWith(`${ROOT_PREFIX}-`)) ||
    !/^[-A-Za-z0-9._]+$/u.test(name)
  ) {
    throw new TypeError("unsafe LVM atomic crash-capture test root");
  }
  return candidate;
}

const ROOT = integrationRoot(
  process.env.LVM_ATOMIC_CRASH_CAPTURE_ROOT ?? `/var/tmp/${ROOT_PREFIX}`,
);
const EXECUTABLES = Object.freeze({
  blockdev:
    process.env.LVM_BLOCKDEV_EXECUTABLE ?? "/usr/sbin/blockdev",
  dmsetup: process.env.LVM_DMSETUP_EXECUTABLE ?? "/usr/sbin/dmsetup",
  losetup: process.env.LVM_LOSETUP_EXECUTABLE ?? "/usr/sbin/losetup",
  lvcreate: process.env.LVM_LVCREATE_EXECUTABLE ?? "/usr/sbin/lvcreate",
  lvs: process.env.LVM_LVS_EXECUTABLE ?? "/usr/sbin/lvs",
  pvcreate: process.env.LVM_PVCREATE_EXECUTABLE ?? "/usr/sbin/pvcreate",
  pvremove: process.env.LVM_PVREMOVE_EXECUTABLE ?? "/usr/sbin/pvremove",
  vgcreate: process.env.LVM_VGCREATE_EXECUTABLE ?? "/usr/sbin/vgcreate",
  vgremove: process.env.LVM_VGREMOVE_EXECUTABLE ?? "/usr/sbin/vgremove",
});
const ORIGIN_SIZE_BYTES = 64 * 1024 * 1024;
const SNAPSHOT_COW_REQUEST_BYTES = 16 * 1024 * 1024 + 1;
const BACKING_SIZE_BYTES = 160 * 1024 * 1024;
const MAX_DIAGNOSTIC_TEXT_BYTES = 1024;
const MAX_COMMAND_DIAGNOSTICS = 32;

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
}

function deepFreeze(value) {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}

function boundedDiagnosticText(value) {
  if (typeof value !== "string") return "";
  if (Buffer.byteLength(value, "utf8") <= MAX_DIAGNOSTIC_TEXT_BYTES) {
    return value;
  }
  return `${Buffer.from(value, "utf8").subarray(0, MAX_DIAGNOSTIC_TEXT_BYTES).toString("utf8")}...[truncated]`;
}

async function command(executable, args, options = {}) {
  return execFileAsync(executable, args, {
    encoding: "utf8",
    env: { ...process.env, LANG: "C", LC_ALL: "C" },
    killSignal: "SIGKILL",
    maxBuffer: options.maxBuffer ?? 1024 * 1024,
    timeout: COMMAND_TIMEOUT_MILLISECONDS,
  });
}

function baseBackend(backendId) {
  const operation = async () => undefined;
  return Object.freeze({
    backendId,
    capabilities: Object.freeze({
      atomicPointInTimeCheckpoint: false,
      exclusiveWriterAttachment: true,
      fencing: "manual",
      normalDirectoryAttachment: true,
    }),
    captureCheckpoint: operation,
    contractVersion: 1,
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    provisionSession: operation,
    restoreCheckpoint: operation,
  });
}

function requestFixture({ backendId, rootPath }) {
  const sessionId = randomUUID();
  const threadId = randomUUID();
  const suffix = randomUUID().replaceAll("-", "");
  const storageId = `lvm-storage-${suffix}`;
  const checkpointId = `lvm-checkpoint-${suffix}`;
  const artifactId = `lvm-artifact-${suffix}`;
  const leaseId = `lvm-lease-${suffix}`;
  const holderId = `lvm-host-${suffix}`;
  return {
    captureAttemptId: `lvm-capture-${suffix}`,
    checkpoint: {
      artifactId,
      backendId,
      checkpointClass: "crash-prefix",
      checkpointId,
      codexSessionId: threadId,
      codexThreadId: threadId,
      contractVersion: 1,
      createdAt: new Date().toISOString(),
      imageDigest: `sha256:${"a".repeat(64)}`,
      sessionId,
      sourceFencingEpoch: "1",
      storageId,
    },
    contractVersion: 1,
    mutationRequest: {
      backendId,
      contractVersion: 1,
      fencingEpoch: "1",
      holderId,
      leaseId,
      operation: "checkpoint",
      operationId: `lvm-operation-${suffix}`,
      sessionId,
      storageId,
      target: { artifactId, checkpointId, kind: "checkpoint" },
    },
    sourceAttachment: {
      attachmentId: `lvm-attachment-${suffix}`,
      backendId,
      contractVersion: 1,
      fencingEpoch: "1",
      holderId,
      kind: "directory",
      leaseId,
      mode: "read-write",
      operationId: `lvm-attach-operation-${suffix}`,
      proofId: `lvm-attach-proof-${suffix}`,
      rootPath,
      sessionId,
      storageId,
    },
    storageRef: { backendId, contractVersion: 1, sessionId, storageId },
  };
}

async function writeDurableState(path, state) {
  const pending = `${path}.pending`;
  await writeFile(pending, `${JSON.stringify(state)}\n`, {
    encoding: "utf8",
    flag: "wx",
    mode: 0o600,
  });
  await rename(pending, path);
}

async function readDurableState(path) {
  try {
    return deepFreeze(JSON.parse(await readFile(path, "utf8")));
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

function createFileCatalogue(path) {
  const claims = new WeakSet();
  const catalogue = exact({
    async claimStarting({ providerBinding, request }) {
      const observed = await readDurableState(path);
      if (observed?.state === "committed") {
        return exact({
          outcome: "committed",
          providerBinding: deepFreeze(observed.providerBinding),
          result: deepFreeze(observed.result),
        });
      }
      if (observed !== null) return exact({ outcome: "unknown" });
      await writeDurableState(path, {
        providerBinding,
        request,
        result: null,
        state: "starting",
      });
      const dispatchClaim = Object.freeze(Object.create(null));
      claims.add(dispatchClaim);
      return exact({ dispatchClaim, outcome: "dispatch" });
    },
    async commitResult({ dispatchClaim, result }) {
      assert.equal(claims.delete(dispatchClaim), true);
      const observed = await readDurableState(path);
      assert.equal(observed.state, "starting");
      await writeDurableState(path, {
        providerBinding: observed.providerBinding,
        request: observed.request,
        result,
        state: "committed",
      });
      return exact({
        outcome: "committed",
        providerBinding: deepFreeze(observed.providerBinding),
        result: deepFreeze(JSON.parse(JSON.stringify(result))),
      });
    },
    contractVersion: 1,
    async markUncertain({ dispatchClaim }) {
      assert.equal(claims.delete(dispatchClaim), true);
      const observed = await readDurableState(path);
      await writeDurableState(path, { ...observed, state: "uncertain" });
      return exact({ outcome: "uncertain" });
    },
    async readCommitted() {
      const observed = await readDurableState(path);
      if (observed?.state !== "committed") return exact({ outcome: "unknown" });
      return exact({
        outcome: "committed",
        providerBinding: deepFreeze(observed.providerBinding),
        result: deepFreeze(observed.result),
      });
    },
  });
  return exact({
    catalogue,
    async close() {},
    kind: "file",
  });
}

async function openCatalogue(filePath) {
  if (DATABASE_URL === null) return createFileCatalogue(filePath);
  const pool = new Pool({
    application_name: "portable-codex-runtime-lvm-atomic-integration",
    connectionString: DATABASE_URL,
    max: 2,
  });
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 2,
  });
  await store.migrate();
  return exact({
    catalogue: createPostgresAtomicCrashCaptureCatalogue({ store }),
    async close() {
      await pool.end();
    },
    kind: "postgres",
  });
}

async function originLvUuid(lvPath) {
  const completion = await command(EXECUTABLES.lvs, [
    "--noheadings",
    "--options",
    "lv_uuid",
    "--",
    lvPath,
  ]);
  const uuid = completion.stdout.trim();
  assert.match(uuid, /^[A-Za-z0-9][A-Za-z0-9-]{5,127}$/u);
  return uuid;
}

test(
  "stopped-only LVM capture survives provider and catalogue restart",
  {
    skip: !ENABLED || process.platform !== "linux",
    timeout: 180_000,
  },
  async (t) => {
    assert.equal(typeof process.getuid === "function" && process.getuid(), 0);
    await mkdir(ROOT, { mode: 0o700, recursive: true });
    const directory = await mkdtemp(join(ROOT, "capture-"));
    const backingPath = join(directory, "lvm-backing.img");
    const sourceRoot = join(directory, "source-root");
    const cataloguePath = join(directory, "catalogue.json");
    const suffix = randomUUID().replaceAll("-", "").slice(0, 16);
    const vgName = `pcrvg${suffix}`;
    const originName = "origin";
    const originPath = `/dev/${vgName}/${originName}`;
    let loopDevice = null;
    let vgCreated = false;
    let firstCatalogue = null;
    let restartedCatalogue = null;

    t.after(async () => {
      await restartedCatalogue?.close().catch(() => {});
      await firstCatalogue?.close().catch(() => {});
      if (vgCreated) {
        await command(EXECUTABLES.vgremove, ["--force", "--yes", vgName])
          .catch(() => {});
      }
      if (loopDevice !== null) {
        await command(EXECUTABLES.pvremove, [
          "--force",
          "--force",
          "--yes",
          loopDevice,
        ]).catch(() => {});
        await command(EXECUTABLES.losetup, ["--detach", loopDevice])
          .catch(() => {});
      }
      await rm(directory, { force: true, recursive: true });
      await rmdir(ROOT).catch((error) => {
        if (error?.code !== "ENOENT" && error?.code !== "ENOTEMPTY") {
          throw error;
        }
      });
    });

    const backing = await open(backingPath, "wx", 0o600);
    await backing.truncate(BACKING_SIZE_BYTES);
    await backing.close();
    await mkdir(sourceRoot, { mode: 0o700 });
    loopDevice = (
      await command(EXECUTABLES.losetup, [
        "--find",
        "--show",
        "--nooverlap",
        backingPath,
      ])
    ).stdout.trim();
    assert.match(loopDevice, /^\/dev\/loop[0-9]+$/u);
    await command(EXECUTABLES.pvcreate, [
      "--force",
      "--yes",
      loopDevice,
    ]);
    await command(EXECUTABLES.vgcreate, [vgName, loopDevice]);
    vgCreated = true;
    await command(EXECUTABLES.lvcreate, [
      "--name",
      originName,
      "--size",
      `${ORIGIN_SIZE_BYTES}B`,
      vgName,
    ]);

    const payload = Buffer.from(
      "portable-codex-runtime stopped LVM atomic capture\n",
      "utf8",
    );
    const origin = await open(originPath, "r+");
    await origin.write(payload, 0, payload.length, 4096);
    await origin.sync();
    await origin.close();
    const persistentOriginUuid = await originLvUuid(originPath);
    const backendId = `lvm-atomic-${suffix}`;
    const request = requestFixture({ backendId, rootPath: sourceRoot });

    let resolveCalls = 0;
    let authorityCalls = 0;
    let snapshotLvcreateCalls = 0;
    const commandDiagnostics = [];
    const commandRunner = async (executable, args, options) => {
      if (executable === EXECUTABLES.lvcreate && args[0] === "--snapshot") {
        snapshotLvcreateCalls += 1;
      }
      try {
        const completion = await command(executable, args, options);
        if (commandDiagnostics.length < MAX_COMMAND_DIAGNOSTICS) {
          commandDiagnostics.push({
            args: [...args],
            executable,
            status: "ok",
            stderr: boundedDiagnosticText(completion.stderr),
            stdout: boundedDiagnosticText(completion.stdout),
          });
        }
        return completion;
      } catch (error) {
        if (commandDiagnostics.length < MAX_COMMAND_DIAGNOSTICS) {
          commandDiagnostics.push({
            args: [...args],
            code: error?.code ?? null,
            executable,
            signal: error?.signal ?? null,
            status: "failed",
            stderr: boundedDiagnosticText(error?.stderr),
            stdout: boundedDiagnosticText(error?.stdout),
          });
        }
        throw error;
      }
    };
    const driver = createLvmAtomicCrashCaptureDriver({
      blockdevExecutable: EXECUTABLES.blockdev,
      commandRunner,
      dmsetupExecutable: EXECUTABLES.dmsetup,
      lvcreateExecutable: EXECUTABLES.lvcreate,
      lvsExecutable: EXECUTABLES.lvs,
      resolveOrigin({ request: presented }) {
        resolveCalls += 1;
        assert.equal(presented.captureAttemptId, request.captureAttemptId);
        return exact({
          originLvUuid: persistentOriginUuid,
          snapshotSizeBytes: String(SNAPSHOT_COW_REQUEST_BYTES),
        });
      },
    });

    const lease = {
      contractVersion: 1,
      expiresAt: "2099-01-01T00:00:00.000Z",
      fencingEpoch: request.sourceAttachment.fencingEpoch,
      holderId: request.sourceAttachment.holderId,
      leaseId: request.sourceAttachment.leaseId,
      sessionId: request.sourceAttachment.sessionId,
    };
    const coordinator = new StoppedWriterCapabilityCoordinator();
    const processIncarnationId = `lvm-process-${suffix}`;
    const writerIncarnationId = `lvm-writer-${suffix}`;
    const stopOperationId = `lvm-stop-${suffix}`;
    const writer = coordinator.registerWriter({
      attachment: request.sourceAttachment,
      canonicalLease: lease,
      processIncarnationId,
      stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
      writerIncarnationId,
    });
    const stoppedCapability = await coordinator.stopAndIssueCapability({
      processIncarnationId,
      stopOperationId,
      writer,
      writerIncarnationId,
    });
    const authorityConsumer = async (admission, runCapture) => {
      authorityCalls += 1;
      assert.strictEqual(admission.captureAuthority, stoppedCapability);
      assert.equal(
        admission.request.captureAttemptId,
        request.captureAttemptId,
      );
      return coordinator.consumeCapability({
        attachment: request.sourceAttachment,
        canonicalLease: lease,
        capability: stoppedCapability,
        processIncarnationId,
        runSnapshot: async () => runCapture(),
        stopOperationId,
        writer,
        writerIncarnationId,
      });
    };

    try {
      firstCatalogue = await openCatalogue(cataloguePath);
      const firstProvider = createLvmAtomicCrashCaptureProvider({
        authorityConsumer,
        baseBackend: baseBackend(backendId),
        catalogue: firstCatalogue.catalogue,
        driver,
      });
      const prepared = prepareAtomicCrashCapture({
        backend: firstProvider,
        request,
      });
      const captured = await capturePreparedAtomicCrashCheckpoint({
        captureAuthority: stoppedCapability,
        preparedCapture: prepared,
      });
      assert.equal(captured.artifact.objectIdentityScheme, "lvm-lv-uuid-v1");
      assert.equal(captured.artifact.readOnly, true);
      assert.equal(captured.artifact.byteLength, String(ORIGIN_SIZE_BYTES));
      assert.notEqual(
        captured.artifact.byteLength,
        String(SNAPSHOT_COW_REQUEST_BYTES),
      );
      assert.equal(snapshotLvcreateCalls, 1);
      assert.equal(authorityCalls, 1);
      assert.equal(resolveCalls, 1);

      await firstCatalogue.close();
      firstCatalogue = null;
      await rm(sourceRoot, { force: true, recursive: true });
      await assert.rejects(open(sourceRoot, "r"), { code: "ENOENT" });

      restartedCatalogue = await openCatalogue(cataloguePath);
      const restartedProvider = createLvmAtomicCrashCaptureProvider({
        authorityConsumer: async () => {
          throw new Error("committed replay must not consume authority");
        },
        baseBackend: baseBackend(backendId),
        catalogue: restartedCatalogue.catalogue,
        driver,
      });
      const verification = await verifyCommittedAtomicCrashCapture({
        backend: restartedProvider,
        request,
      });
      assert.equal(verification.outcome, "committed");
      assert.deepEqual(verification.result, captured);
      assert.equal(resolveCalls, 1);
      assert.equal(authorityCalls, 1);
      assert.equal(snapshotLvcreateCalls, 1);

      const replayPrepared = prepareAtomicCrashCapture({
        backend: restartedProvider,
        request,
      });
      const replayed = await capturePreparedAtomicCrashCheckpoint({
        captureAuthority: exact({ unusableAfterRestart: true }),
        preparedCapture: replayPrepared,
      });
      assert.deepEqual(replayed, captured);
      assert.equal(resolveCalls, 2);
      assert.equal(authorityCalls, 1);
      assert.equal(snapshotLvcreateCalls, 1);
      assert.equal(firstCatalogue?.kind ?? restartedCatalogue.kind,
        DATABASE_URL === null ? "file" : "postgres");
    } catch (error) {
      t.diagnostic(JSON.stringify({
        authorityCalls,
        commandDiagnostics,
        resolveCalls,
        snapshotLvcreateCalls,
      }));
      throw error;
    }
  },
);
