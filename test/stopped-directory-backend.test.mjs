import assert from "node:assert/strict";
import {
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { runInNewContext } from "node:vm";

import {
  STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION,
  StoppedDirectoryBackend,
  StoppedDirectoryBackendError,
} from "../src/stopped-directory-backend.mjs";
import { FilesystemOperationJournal } from "../src/filesystem-operation-journal.mjs";
import {
  SessionSnapshotCoreError,
  captureCleanCheckpoint,
  reconcileCleanCheckpointCapture,
  restoreCleanCheckpoint,
} from "../src/session-snapshot-core.mjs";
import {
  assertStorageBackend,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";
import {
  StoppedDirectoryPublication,
  stoppedDirectoryPublicationCandidateName,
} from "../src/stopped-directory-publication.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
} from "../src/stopped-writer-capability.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const BACKEND_ID = "single-attach-test";
const STORAGE_ID = "volume-001";
const RESTORE_STORAGE_ID = "volume-002";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const CAPTURE_OPERATION_ID = "operation-checkpoint-001";
const RESTORE_OPERATION_ID = "operation-restore-001";
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
const STOP_OPERATION_ID = "stop-operation-001";
const RESERVATION_ID = "reservation-001";
const CAPTURE_ATTEMPT_ID = "capture-attempt-001";
const DESTINATION_ISOLATION_PROOF_ID = "destination-isolation-proof-001";
const NOW = Date.parse("2026-07-02T12:00:00.000Z");
const CREATED_AT = "2026-07-02T12:00:00.000Z";
const TEST_OBJECT_IDENTITY_SCHEME = "test-object-generation-v1";

const TRUSTED_JOURNAL_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectDirectoryAcl: async () => false,
});

const TRUSTED_PUBLICATION_INSPECTORS = Object.freeze({
  inspectOwnedRootAcl: async () => false,
  inspectOwnedRootAncestorAcl: async () => false,
  listMountPoints: async () => ["/"],
});

function manifest() {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest: IMAGE_DIGEST,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
      codexVersion: "codex-cli 0.142.4",
      codexSandbox: "danger-full-access",
    },
  });
}

function storageRef(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    ...overrides,
  };
}

function lease(overrides = {}) {
  return {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    expiresAt: "2026-07-02T12:01:00.000Z",
    ...overrides,
  };
}

function restoreLease(overrides = {}) {
  return lease({
    leaseId: "lease-002",
    holderId: "host-002",
    fencingEpoch: "12",
    ...overrides,
  });
}

function attachment(writerLease = lease(), overrides = {}) {
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    attachmentId: "attachment-001",
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operationId: "operation-attach-001",
    proofId: "proof-attachment-001",
    kind: "directory",
    rootPath: "/var/lib/portable-codex/session-001",
    mode: "read-write",
    ...overrides,
  };
}

function mutationRequest(operation, writerLease, overrides = {}) {
  const isRestore = operation === "restore";
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: isRestore ? RESTORE_STORAGE_ID : STORAGE_ID,
    sessionId: SESSION_ID,
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operation,
    operationId: isRestore ? RESTORE_OPERATION_ID : CAPTURE_OPERATION_ID,
    target: {
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    },
    ...overrides,
  };
}

function checkpoint(overrides = {}) {
  return {
    contractVersion: 1,
    checkpointId: CHECKPOINT_ID,
    artifactId: ARTIFACT_ID,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    codexThreadId: THREAD_ID,
    codexSessionId: THREAD_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "11",
    checkpointClass: "clean",
    createdAt: CREATED_AT,
    ...overrides,
  };
}

function mutationResult(request) {
  return {
    ...request,
    proofId: `proof-${request.operation}-001`,
    status: request.operation === "checkpoint" ? "checkpoint-created" : "restored",
  };
}

function fixedResult(checkpointDescriptor, request) {
  return Object.freeze({
    checkpoint: checkpointDescriptor,
    mutation: mutationResult(request),
  });
}

function captureAttemptBinding(
  fixture,
  admission,
  captureAttemptId = CAPTURE_ATTEMPT_ID,
) {
  return Object.freeze({
    attachmentId: fixture.writerAttachment.attachmentId,
    attachmentOperationId: fixture.writerAttachment.operationId,
    attachmentProofId: fixture.writerAttachment.proofId,
    captureAttemptId,
    checkpoint: admission.checkpoint,
    contractVersion: STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION,
    processIncarnationId: PROCESS_INCARNATION_ID,
    reservationId: RESERVATION_ID,
    stopOperationId: STOP_OPERATION_ID,
    writerIncarnationId: WRITER_INCARNATION_ID,
  });
}

function captureAttemptRecord(fixture, admission, overrides = {}) {
  const captureAttemptId = overrides.captureAttemptId ?? CAPTURE_ATTEMPT_ID;
  return Object.freeze({
    binding:
      overrides.binding ??
      captureAttemptBinding(fixture, admission, captureAttemptId),
    captureAttemptId,
    contractVersion: 1,
    operationId: admission.request.operationId,
    request: admission.request,
    result: fixedResult(admission.checkpoint, admission.request),
    state: "authorized",
    ...overrides,
  });
}

function durableCaptureCompletion(completion) {
  return Object.freeze({
    artifactProof: completion.artifactProof,
    materialization: completion.materialization,
    result: completion.result,
  });
}

function exactKeys(value, expected) {
  assert.deepEqual(Object.keys(value).sort(), [...expected].sort());
}

function deferred() {
  let resolve;
  const promise = new Promise((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

function adversarialAuthorityPromiseScenarios(observation) {
  class ForeignPromise extends Promise {}
  const pendingNativePromise = () => new Promise(() => {});
  const publishThen = (publish, context) =>
    function then(resolve, reject) {
      observation.thenCalls += 1;
      publish(context).then(resolve, reject);
    };
  return [
    {
      name: "constructor accessor",
      create({ context, publish }) {
        const value = pendingNativePromise();
        Object.defineProperty(value, "constructor", {
          configurable: true,
          get() {
            observation.constructorReads += 1;
            return ForeignPromise;
          },
        });
        Object.defineProperty(value, "then", {
          configurable: true,
          value: publishThen(publish, context),
        });
        return value;
      },
    },
    {
      name: "foreign constructor data",
      create({ context, publish }) {
        const value = pendingNativePromise();
        Object.defineProperty(value, "constructor", {
          configurable: true,
          value: ForeignPromise,
        });
        Object.defineProperty(value, "then", {
          configurable: true,
          value: publishThen(publish, context),
        });
        return value;
      },
    },
    {
      name: "Promise subclass",
      create({ context, publish }) {
        const value = new ForeignPromise(() => {});
        Object.defineProperty(value, "then", {
          configurable: true,
          value: publishThen(publish, context),
        });
        return value;
      },
    },
    {
      name: "cross-realm Promise",
      create({ context, publish }) {
        const value = runInNewContext("new Promise(() => {})");
        Object.defineProperty(value, "then", {
          configurable: true,
          value: publishThen(publish, context),
        });
        return value;
      },
    },
    {
      name: "proxy prototype",
      create({ context, publish }) {
        const value = pendingNativePromise();
        const then = publishThen(publish, context);
        const prototype = new Proxy(Object.getPrototypeOf(value), {
          get(target, property, receiver) {
            observation.prototypeTraps += 1;
            if (property === "constructor") return ForeignPromise;
            if (property === "then") return then;
            return Reflect.get(target, property, receiver);
          },
        });
        Object.setPrototypeOf(value, prototype);
        return value;
      },
    },
  ];
}

function destinationChangedError() {
  const error = new Error("rename destination changed");
  error.code = "destination_changed";
  Object.defineProperty(error, "renameOutcome", { value: "not-committed" });
  return error;
}

function simpleLockProvider() {
  return async () => ({
    async assertHeld() {},
    async release() {},
    async renameWhileHeld(source, destination, expectedDestination) {
      if (expectedDestination?.kind === "absent") {
        try {
          await lstat(destination);
        } catch (error) {
          if (error?.code !== "ENOENT") throw destinationChangedError();
          await rename(source, destination);
          return;
        }
        throw destinationChangedError();
      }
      await rename(source, destination);
    },
  });
}

async function pathExists(path) {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

async function inspectTestPersistentObjectIdentity(path) {
  const metadata = await lstat(path, { bigint: true });
  return {
    device: metadata.dev.toString(),
    inode: metadata.ino.toString(),
    objectId: `test-object-${metadata.dev}-${metadata.ino}-${metadata.birthtimeNs}`,
  };
}

function createLifecycleBackend() {
  const calls = [];
  const delegatedResult = Object.freeze({ delegated: true });
  const backend = {
    contractVersion: 1,
    backendId: BACKEND_ID,
    capabilities: {
      atomicPointInTimeCheckpoint: false,
      exclusiveWriterAttachment: true,
      fencing: "manual",
      normalDirectoryAttachment: true,
    },
    async captureCheckpoint() {
      throw new Error("capture must be owned by the adapter");
    },
    async restoreCheckpoint() {
      throw new Error("restore must be owned by the adapter");
    },
  };
  for (const method of [
    "provisionSession",
    "prepareWritableAttachment",
    "detachAttachment",
    "forceFence",
    "destroySession",
  ]) {
    backend[method] = async function delegate(input) {
      assert.strictEqual(this, backend);
      calls.push({ input, method });
      return delegatedResult;
    };
  }
  return { backend, calls, delegatedResult };
}

function assertBackendError(error, code = "stopped_directory_backend_outcome_uncertain") {
  const messages = {
    invalid_stopped_directory_backend_request:
      "Stopped-directory backend request is invalid",
    stopped_directory_backend_outcome_uncertain:
      "Stopped-directory backend outcome is uncertain",
  };
  assert(error instanceof StoppedDirectoryBackendError);
  assert.strictEqual(
    Object.getPrototypeOf(error),
    StoppedDirectoryBackendError.prototype,
  );
  assert.equal(error.name, "StoppedDirectoryBackendError");
  assert.equal(error.code, code);
  assert.equal(error.message, messages[code]);
  assert.equal(error.retryable, false);
  assert.equal(
    error.stack,
    `StoppedDirectoryBackendError: ${messages[code]}`,
  );
  assert.equal(Object.isFrozen(error), true);
  const ownKeys = Reflect.ownKeys(error);
  assert.equal(ownKeys.every((key) => typeof key === "string"), true);
  assert.deepEqual([...ownKeys].sort(), [
    "code",
    "message",
    "name",
    "retryable",
    "stack",
  ]);
  return true;
}

function assertCoreError(error, code) {
  assert(error instanceof SessionSnapshotCoreError);
  assert.equal(error.code, code);
  assert.equal(error.retryable, false);
  assert.equal(Object.hasOwn(error, "cause"), false);
  assert.equal(Object.hasOwn(error, "details"), false);
  return true;
}

async function createFixture(t, options = {}) {
  const root = await mkdtemp(join(tmpdir(), "stopped-directory-backend-test-"));
  const sourceOwnedRoot = join(root, "source-root");
  const artifactOwnedRoot = join(root, "artifact-root");
  const destinationOwnedRoot = join(root, "destination-root");
  const journalDirectory = join(root, "journal");
  for (const directory of [
    sourceOwnedRoot,
    artifactOwnedRoot,
    destinationOwnedRoot,
    journalDirectory,
  ]) {
    await mkdir(directory, { mode: 0o700 });
  }
  t.after(() => rm(root, { force: true, recursive: true }));

  const sourceDirectory = join(sourceOwnedRoot, "session");
  await mkdir(join(sourceDirectory, "workspace", "nested"), {
    mode: 0o700,
    recursive: true,
  });
  await writeFile(join(sourceDirectory, "workspace", "README.md"), "portable\n", {
    mode: 0o640,
  });
  await writeFile(
    join(sourceDirectory, "workspace", "nested", "state.jsonl"),
    '{"type":"turn","state":"completed"}\n',
    { mode: 0o600 },
  );
  await symlink("README.md", join(sourceDirectory, "workspace", "current"));

  const observation = {
    captureGuard: false,
    events: [],
    preseeding: false,
    reconciliationGuard: false,
    restoreGuard: false,
  };
  const journal = new FilesystemOperationJournal({
    directory: journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: simpleLockProvider(),
    faults: {
      afterJournalPrepared: async () => {
        assert.equal(
          observation.captureGuard ||
            observation.reconciliationGuard ||
            observation.restoreGuard ||
            observation.preseeding,
          true,
          "publication must remain inside the mutation-authority guard",
        );
        observation.events.push("publication:prepared");
        await options.publicationFaults?.afterJournalPrepared?.();
      },
      afterCopy: async () => {
        assert.equal(
          observation.captureGuard ||
            observation.reconciliationGuard ||
            observation.restoreGuard ||
            observation.preseeding,
          true,
          "copy must remain inside the mutation-authority guard",
        );
        observation.events.push("publication:copied");
        await options.publicationFaults?.afterCopy?.();
      },
      afterMaterialized: async () => {
        assert.equal(
          observation.captureGuard ||
            observation.reconciliationGuard ||
            observation.restoreGuard ||
            observation.preseeding,
          true,
          "materialization must remain inside the mutation-authority guard",
        );
        await options.publicationFaults?.afterMaterialized?.();
      },
    },
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_INSPECTORS,
  });

  const fixture = {
    artifactDirectory: join(artifactOwnedRoot, ARTIFACT_ID),
    artifactOwnedRoot,
    destinationDirectory: join(destinationOwnedRoot, "restored-session"),
    destinationOwnedRoot,
    journal,
    journalDirectory,
    observation,
    publication,
    root,
    sourceDirectory,
    sourceOwnedRoot,
  };
  return createRuntime(fixture, options);
}

function createMutationAuthority(fixture, options = {}) {
  const state = {
    background: [],
    captureAdmissions: [],
    captureAttempts: new Map(),
    captureCallbackCompletions: [],
    captureCatalogue: new Map(),
    captureContexts: [],
    captureFinalizations: [],
    captureRuns: 0,
    reconciliationAdmissions: [],
    activeReconciliations: 0,
    maxActiveReconciliations: 0,
    reconciliationCallbackCompletions: [],
    reconciliationContexts: [],
    reconciliationFinalizations: [],
    reconciliationRuns: 0,
    reconciliationTails: new Map(),
    restoreAdmissions: [],
    restoreCallbackCompletions: [],
    restoreContexts: [],
    restoreFinalizations: [],
    restoreRuns: 0,
  };

  const captureContext = (admission) => {
    const context = {
      artifactDirectory: fixture.artifactDirectory,
      artifactOwnedRoot: fixture.artifactOwnedRoot,
      canonicalAttachment: fixture.writerAttachment,
      canonicalLease: fixture.writerLease,
      captureAttemptId: CAPTURE_ATTEMPT_ID,
      now: NOW,
      reservationId: RESERVATION_ID,
      result: fixedResult(admission.checkpoint, admission.request),
      sourceDirectory: fixture.sourceDirectory,
      sourceOwnedRoot: fixture.sourceOwnedRoot,
      storageRef: storageRef(),
      ...(typeof options.captureContext === "function"
        ? options.captureContext(admission)
        : options.captureContext),
    };
    return Object.freeze(context);
  };

  const reconciliationContext = (admission) => {
    const context = {
      artifactDirectory: fixture.artifactDirectory,
      artifactOwnedRoot: fixture.artifactOwnedRoot,
      captureAttempt: state.captureAttempts.get(admission.request.operationId),
      ...(typeof options.reconciliationContext === "function"
        ? options.reconciliationContext(admission, state)
        : options.reconciliationContext),
    };
    return Object.freeze(context);
  };

  const restoreContext = (admission) => {
    const context = {
      artifactDirectory: fixture.artifactDirectory,
      artifactOwnedRoot: fixture.artifactOwnedRoot,
      artifactProof: fixture.artifactProof,
      canonicalLease: fixture.restoreWriterLease,
      destinationDirectory: fixture.destinationDirectory,
      destinationIsolationProofId: DESTINATION_ISOLATION_PROOF_ID,
      destinationOwnedRoot: fixture.destinationOwnedRoot,
      destinationState: "detached",
      now: NOW,
      reservationId: "reservation-restore-001",
      result: fixedResult(admission.checkpoint, admission.request),
      storageRef: storageRef({ storageId: RESTORE_STORAGE_ID }),
      ...(typeof options.restoreContext === "function"
        ? options.restoreContext(admission)
        : options.restoreContext),
    };
    return Object.freeze(context);
  };

  const authorizeCaptureAttempt = (admission, context) => {
    assert.equal(
      state.captureAttempts.has(admission.request.operationId),
      false,
      "normal capture must durably create a fresh attempt",
    );
    const attempt = captureAttemptRecord(fixture, admission, {
      captureAttemptId: context.captureAttemptId,
    });
    state.captureAttempts.set(admission.request.operationId, attempt);
    fixture.observation.events.push("authority:capture:attempt-authorized");
    options.onCaptureAttemptAuthorized?.(attempt);
    return attempt;
  };

  const finalizeCaptureAttempt = (admission, completion) => {
    const current = state.captureAttempts.get(admission.request.operationId);
    assert(current, "capture attempt must remain durable through finalization");
    const durableCompletion = durableCaptureCompletion(completion);
    const existing = state.captureCatalogue.get(admission.request.operationId);
    if (existing !== undefined) {
      assert.deepEqual(durableCompletion, existing);
    } else {
      state.captureCatalogue.set(
        admission.request.operationId,
        durableCompletion,
      );
    }
    state.captureAttempts.set(
      admission.request.operationId,
      Object.freeze({ ...current, state: "committed" }),
    );
  };

  const runReconciliationSerialized = async (operationId, operation) => {
    const preceding =
      state.reconciliationTails.get(operationId) ?? Promise.resolve();
    const current = preceding.then(operation, operation);
    state.reconciliationTails.set(operationId, current);
    try {
      return await current;
    } finally {
      if (state.reconciliationTails.get(operationId) === current) {
        state.reconciliationTails.delete(operationId);
      }
    }
  };

  const normalAuthority = {
    async runCapture(admission, publish) {
      state.captureRuns += 1;
      state.captureAdmissions.push(admission);
      fixture.observation.events.push("authority:capture:start");
      assert(Object.isFrozen(admission));
      exactKeys(admission, [
        "attachment",
        "checkpoint",
        "processIncarnationId",
        "request",
        "stopOperationId",
        "writerIncarnationId",
      ]);
      assert.deepEqual(admission.attachment, fixture.writerAttachment);
      assert.equal(admission.processIncarnationId, PROCESS_INCARNATION_ID);
      assert.equal(admission.writerIncarnationId, WRITER_INCARNATION_ID);
      assert.equal(admission.stopOperationId, STOP_OPERATION_ID);
      assert.equal(Object.hasOwn(admission, "capability"), false);
      assert.equal(Object.hasOwn(admission, "writer"), false);

      const context = captureContext(admission);
      state.captureContexts.push(context);
      fixture.observation.captureGuard = true;
      try {
        authorizeCaptureAttempt(admission, context);
        options.onCaptureEntered?.();
        if (options.captureGate) await options.captureGate;
        switch (options.captureMode) {
          case "zero":
            return Object.freeze({ ignored: true });
          case "multiple": {
            const first = await publish(context);
            state.captureCallbackCompletions.push(first);
            const second = await publish(context);
            state.captureCallbackCompletions.push(second);
            return first;
          }
          case "early": {
            const pending = publish(context);
            state.background.push(pending.catch(() => undefined));
            return Object.freeze({ ignored: true });
          }
          case "discard-invalid": {
            void publish(Object.freeze({}));
            return Object.freeze({ ignored: true });
          }
          case "late-unobserved": {
            setImmediate(() => {
              void publish(context);
              options.onCaptureLatePublishIssued?.();
            });
            return Object.freeze({ ignored: true });
          }
          case "throw":
            throw new Error(
              "runtime secret Bearer sensitive-token-value at /company/private/session",
            );
          case "substituted": {
            const completion = await publish(context);
            state.captureCallbackCompletions.push(completion);
            return Object.freeze({ ...completion });
          }
          default: {
            const completion = await publish(context);
            state.captureCallbackCompletions.push(completion);
            exactKeys(completion, [
              "artifactProof",
              "materialization",
              "replayed",
              "result",
            ]);
            assert(Object.isFrozen(completion));
            assert(Object.isFrozen(completion.artifactProof));
            assert.deepEqual(completion.result, context.result);
            options.onCapturePublished?.(completion);
            if (options.captureFinalizationGate) {
              await options.captureFinalizationGate;
            }
            if (options.captureFinalizationFailure) {
              throw new Error("capture catalogue finalization acknowledgement lost");
            }
            finalizeCaptureAttempt(admission, completion);
            fixture.artifactProof = completion.artifactProof;
            state.captureFinalizations.push(completion);
            fixture.observation.events.push("authority:capture:finalized");
            return completion;
          }
        }
      } finally {
        fixture.observation.captureGuard = false;
        fixture.observation.events.push("authority:capture:end");
      }
    },

    async runCaptureReconciliation(admission, verify) {
      state.reconciliationRuns += 1;
      state.reconciliationAdmissions.push(admission);
      fixture.observation.events.push("authority:reconciliation:start");
      assert(Object.isFrozen(admission));
      exactKeys(admission, ["checkpoint", "request"]);
      assert.equal(Object.hasOwn(admission, "attachment"), false);
      assert.equal(Object.hasOwn(admission, "capability"), false);
      assert.equal(Object.hasOwn(admission, "writer"), false);
      options.onReconciliationAuthorityEntered?.(admission);
      if (options.reconciliationEntryGate) {
        await options.reconciliationEntryGate;
      }

      return runReconciliationSerialized(
        admission.request.operationId,
        async () => {
          const context = reconciliationContext(admission);
          if (context.captureAttempt === undefined) {
            throw new Error("canonical capture attempt is missing");
          }
          state.reconciliationContexts.push(context);
          state.activeReconciliations += 1;
          state.maxActiveReconciliations = Math.max(
            state.maxActiveReconciliations,
            state.activeReconciliations,
          );
          fixture.observation.reconciliationGuard = true;
          try {
            options.onReconciliationEntered?.();
            if (options.reconciliationGate) await options.reconciliationGate;
            switch (options.reconciliationMode) {
              case "zero":
                return Object.freeze({ ignored: true });
              case "multiple": {
                const first = await verify(context);
                state.reconciliationCallbackCompletions.push(first);
                const second = await verify(context);
                state.reconciliationCallbackCompletions.push(second);
                return first;
              }
              case "early": {
                const pending = verify(context);
                state.background.push(pending.catch(() => undefined));
                return Object.freeze({ ignored: true });
              }
              case "late-unobserved": {
                setImmediate(() => {
                  void verify(context);
                  options.onReconciliationLateVerifyIssued?.();
                });
                return Object.freeze({ ignored: true });
              }
              case "substituted": {
                const completion = await verify(context);
                state.reconciliationCallbackCompletions.push(completion);
                return Object.freeze({ ...completion });
              }
              default: {
                const completion = await verify(context);
                state.reconciliationCallbackCompletions.push(completion);
                exactKeys(completion, [
                  "artifactProof",
                  "materialization",
                  "replayed",
                  "result",
                ]);
                assert(Object.isFrozen(completion));
                assert(Object.isFrozen(completion.artifactProof));
                assert.equal(completion.replayed, true);
                options.onReconciliationVerified?.(completion);
                if (options.reconciliationFinalizationGate) {
                  await options.reconciliationFinalizationGate;
                }
                finalizeCaptureAttempt(admission, completion);
                fixture.artifactProof = completion.artifactProof;
                state.reconciliationFinalizations.push(completion);
                fixture.observation.events.push(
                  "authority:reconciliation:finalized",
                );
                return completion;
              }
            }
          } finally {
            fixture.observation.reconciliationGuard = false;
            state.activeReconciliations -= 1;
            fixture.observation.events.push("authority:reconciliation:end");
          }
        },
      );
    },

    async runRestore(admission, publish) {
      state.restoreRuns += 1;
      state.restoreAdmissions.push(admission);
      fixture.observation.events.push("authority:restore:start");
      assert(Object.isFrozen(admission));
      exactKeys(admission, ["checkpoint", "request"]);
      const context = restoreContext(admission);
      state.restoreContexts.push(context);
      fixture.observation.restoreGuard = true;
      try {
        switch (options.restoreMode) {
          case "zero":
            return Object.freeze({ ignored: true });
          case "multiple": {
            const first = await publish(context);
            state.restoreCallbackCompletions.push(first);
            const second = await publish(context);
            state.restoreCallbackCompletions.push(second);
            return first;
          }
          case "early": {
            const pending = publish(context);
            state.background.push(pending.catch(() => undefined));
            return Object.freeze({ ignored: true });
          }
          case "discard-invalid-throw": {
            void publish(Object.freeze({}));
            throw new Error("authority discarded an invalid callback");
          }
          case "late-unobserved": {
            setImmediate(() => {
              void publish(context);
              options.onRestoreLatePublishIssued?.();
            });
            return Object.freeze({ ignored: true });
          }
          case "substituted": {
            const completion = await publish(context);
            state.restoreCallbackCompletions.push(completion);
            return Object.freeze({ ...completion });
          }
          default: {
            const completion = await publish(context);
            state.restoreCallbackCompletions.push(completion);
            exactKeys(completion, ["materialization", "replayed", "result"]);
            assert(Object.isFrozen(completion));
            assert.deepEqual(completion.result, context.result);
            options.onRestorePublished?.(completion);
            if (options.restoreFinalizationGate) {
              await options.restoreFinalizationGate;
            }
            state.restoreFinalizations.push(completion);
            fixture.observation.events.push("authority:restore:finalized");
            return completion;
          }
        }
      } finally {
        fixture.observation.restoreGuard = false;
        fixture.observation.events.push("authority:restore:end");
      }
    },
  };
  const authority = {
    runCapture:
      options.captureReturnFactory === undefined
        ? normalAuthority.runCapture
        : function runCaptureWithAdversarialReturn(admission, publish) {
            state.captureRuns += 1;
            state.captureAdmissions.push(admission);
            const context = captureContext(admission);
            state.captureContexts.push(context);
            fixture.observation.captureGuard = true;
            try {
              authorizeCaptureAttempt(admission, context);
            } finally {
              fixture.observation.captureGuard = false;
            }
            const publishUnderGuard = async (value) => {
              fixture.observation.captureGuard = true;
              try {
                return await publish(value);
              } finally {
                fixture.observation.captureGuard = false;
              }
            };
            return options.captureReturnFactory({
              admission,
              context,
              publish: publishUnderGuard,
            });
          },
    runCaptureReconciliation: normalAuthority.runCaptureReconciliation,
    runRestore:
      options.restoreReturnFactory === undefined
        ? normalAuthority.runRestore
        : function runRestoreWithAdversarialReturn(admission, publish) {
            state.restoreRuns += 1;
            state.restoreAdmissions.push(admission);
            const context = restoreContext(admission);
            state.restoreContexts.push(context);
            const publishUnderGuard = async (value) => {
              fixture.observation.restoreGuard = true;
              try {
                return await publish(value);
              } finally {
                fixture.observation.restoreGuard = false;
              }
            };
            return options.restoreReturnFactory({
              admission,
              context,
              publish: publishUnderGuard,
            });
          },
  };
  return { authority, state };
}

function createRuntime(fixture, options = {}) {
  const writerLease = options.writerLease ?? lease();
  const writerAttachment =
    options.writerAttachment ??
    attachment(writerLease, { rootPath: fixture.sourceDirectory });
  const restoreWriterLease = options.restoreWriterLease ?? restoreLease();
  Object.assign(fixture, {
    restoreWriterLease,
    writerAttachment,
    writerLease,
  });

  const coordinator = options.coordinator ?? new StoppedWriterCapabilityCoordinator();
  let stopCalls = 0;
  const writer = coordinator.registerWriter({
    attachment: writerAttachment,
    canonicalLease: writerLease,
    processIncarnationId: PROCESS_INCARNATION_ID,
    stopWriter(binding) {
      stopCalls += 1;
      assert(Object.isFrozen(binding));
      assert.equal(binding.stopOperationId, STOP_OPERATION_ID);
      return STOPPED_WRITER_STOP_CONFIRMED;
    },
    writerIncarnationId: WRITER_INCARNATION_ID,
  });

  const lifecycle = options.lifecycle ?? createLifecycleBackend();
  const mutation = createMutationAuthority(fixture, options);
  let resolverCalls = 0;
  const resolveDefault = (input) => {
    resolverCalls += 1;
    fixture.observation.events.push("resolver");
    assert(Object.isFrozen(input));
    exactKeys(input, ["attachment", "checkpoint", "request"]);
    return Object.freeze({
      canonicalLeaseAtRegistration: writerLease,
      processIncarnationId: PROCESS_INCARNATION_ID,
      stopOperationId: STOP_OPERATION_ID,
      writer,
      writerIncarnationId: WRITER_INCARNATION_ID,
    });
  };
  const resolveStoppedWriter =
    options.resolveStoppedWriterFactory?.({
      resolveDefault,
      writer,
      writerAttachment,
      writerLease,
    }) ??
    options.resolveStoppedWriter ??
    resolveDefault;

  const backend = new StoppedDirectoryBackend({
    backendId: BACKEND_ID,
    coordinator,
    lifecycleBackend: lifecycle.backend,
    mutationAuthority: mutation.authority,
    publication: fixture.publication,
    resolveStoppedWriter,
  });
  Object.assign(fixture, {
    backend,
    coordinator,
    lifecycle,
    mutation,
    writer,
  });
  Object.defineProperties(fixture, {
    resolverCalls: {
      configurable: true,
      enumerable: true,
      get: () => resolverCalls,
    },
    stopCalls: {
      configurable: true,
      enumerable: true,
      get: () => stopCalls,
    },
  });
  return fixture;
}

async function issueCapability(fixture) {
  return fixture.coordinator.stopAndIssueCapability({
    processIncarnationId: PROCESS_INCARNATION_ID,
    stopOperationId: STOP_OPERATION_ID,
    writer: fixture.writer,
    writerIncarnationId: WRITER_INCARNATION_ID,
  });
}

function captureCoreOptions(fixture, capability, overrides = {}) {
  return {
    attachment: fixture.writerAttachment,
    backend: fixture.backend,
    canonicalLease: fixture.writerLease,
    checkpointClass: "clean",
    createdAt: CREATED_AT,
    manifest: manifest(),
    now: NOW,
    request: mutationRequest("checkpoint", fixture.writerLease),
    stoppedWriterEvidence: capability,
    storageRef: storageRef(),
    ...overrides,
  };
}

function captureDispatchInput(fixture, capability, overrides = {}) {
  return {
    attachment: fixture.writerAttachment,
    checkpoint: checkpoint(),
    request: mutationRequest("checkpoint", fixture.writerLease),
    stoppedWriterEvidence: capability,
    ...overrides,
  };
}

function captureReconciliationInput(fixture, overrides = {}) {
  return {
    checkpoint: checkpoint(),
    request: mutationRequest("checkpoint", fixture.writerLease),
    ...overrides,
  };
}

function capturePublicationOptions(fixture) {
  const captureRequest = mutationRequest("checkpoint", fixture.writerLease);
  return {
    artifactDirectory: fixture.artifactDirectory,
    artifactOwnedRoot: fixture.artifactOwnedRoot,
    binding: {
      attachmentId: fixture.writerAttachment.attachmentId,
      attachmentOperationId: fixture.writerAttachment.operationId,
      attachmentProofId: fixture.writerAttachment.proofId,
      captureAttemptId: CAPTURE_ATTEMPT_ID,
      checkpoint: checkpoint(),
      contractVersion: STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION,
      processIncarnationId: PROCESS_INCARNATION_ID,
      reservationId: RESERVATION_ID,
      stopOperationId: STOP_OPERATION_ID,
      writerIncarnationId: WRITER_INCARNATION_ID,
    },
    operationId: CAPTURE_OPERATION_ID,
    request: captureRequest,
    result: fixedResult(checkpoint(), captureRequest),
    sourceDirectory: fixture.sourceDirectory,
    sourceOwnedRoot: fixture.sourceOwnedRoot,
  };
}

function captureCandidatePath(fixture) {
  return join(
    fixture.artifactOwnedRoot,
    stoppedDirectoryPublicationCandidateName(
      CAPTURE_OPERATION_ID,
      ARTIFACT_ID,
    ),
  );
}

function restoreCoreOptions(fixture, overrides = {}) {
  return {
    backend: fixture.backend,
    canonicalLease: fixture.restoreWriterLease,
    checkpoint: checkpoint(),
    manifest: manifest(),
    now: NOW,
    request: mutationRequest("restore", fixture.restoreWriterLease),
    storageRef: storageRef({ storageId: RESTORE_STORAGE_ID }),
    ...overrides,
  };
}

function restoreDispatchInput(fixture, overrides = {}) {
  return {
    checkpoint: checkpoint(),
    request: mutationRequest("restore", fixture.restoreWriterLease),
    ...overrides,
  };
}

test("backend exposes the fixed directory surface and delegates lifecycle operations", async (t) => {
  const fixture = await createFixture(t);

  assert.equal(STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION, 2);
  assert.strictEqual(assertStorageBackend(fixture.backend), fixture.backend);
  assert.equal(fixture.backend.contractVersion, 1);
  assert.equal(fixture.backend.captureReconciliationContractVersion, 1);
  assert.equal(fixture.backend.backendId, BACKEND_ID);
  assert.deepEqual(fixture.backend.capabilities, {
    atomicPointInTimeCheckpoint: false,
    exclusiveWriterAttachment: true,
    fencing: "manual",
    normalDirectoryAttachment: true,
  });
  assert(Object.isFrozen(fixture.backend));
  assert(Object.isFrozen(fixture.backend.capabilities));

  for (const method of [
    "provisionSession",
    "prepareWritableAttachment",
    "detachAttachment",
    "forceFence",
    "destroySession",
  ]) {
    const input = Object.freeze({ method });
    assert.strictEqual(await fixture.backend[method](input), fixture.lifecycle.delegatedResult);
    assert.deepEqual(fixture.lifecycle.calls.at(-1), { input, method });
  }

  const mismatched = createLifecycleBackend();
  mismatched.backend.backendId = "other-backend";
  assert.throws(
    () =>
      new StoppedDirectoryBackend({
        backendId: BACKEND_ID,
        coordinator: new StoppedWriterCapabilityCoordinator(),
        lifecycleBackend: mismatched.backend,
        mutationAuthority: fixture.mutation.authority,
        publication: fixture.publication,
        resolveStoppedWriter() {},
      }),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );

  assert.throws(
    () =>
      new StoppedDirectoryBackend({
        backendId: BACKEND_ID,
        coordinator: fixture.coordinator,
        lifecycleBackend: fixture.lifecycle.backend,
        mutationAuthority: {
          runCapture: fixture.mutation.authority.runCapture,
          runRestore: fixture.mutation.authority.runRestore,
        },
        publication: fixture.publication,
        resolveStoppedWriter() {},
      }),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
});

test("lifecycle delegation contains collaborator errors without leaking details", async (t) => {
  const lifecycle = createLifecycleBackend();
  const methods = [
    "provisionSession",
    "prepareWritableAttachment",
    "detachAttachment",
    "forceFence",
    "destroySession",
  ];
  const secretPath = "/company/private/lifecycle/session-001";
  const collaboratorMarker = "lifecycle-collaborator-error-marker";
  const fail = (backend, method, input) => {
    assert.strictEqual(backend, lifecycle.backend);
    lifecycle.calls.push({ input, method });
    throw new Error(`${collaboratorMarker} at ${secretPath} for ${method}`);
  };
  for (let index = 0; index < methods.length; index += 1) {
    const method = methods[index];
    lifecycle.backend[method] =
      index % 2 === 0
        ? function failSynchronously(input) {
            return fail(this, method, input);
          }
        : async function failAsynchronously(input) {
            return fail(this, method, input);
          };
  }
  const fixture = await createFixture(t, { lifecycle });

  for (const method of methods) {
    const input = Object.freeze({ method });
    let observed;
    await assert.rejects(
      () => fixture.backend[method](input),
      (error) => {
        observed = error;
        return assertBackendError(error);
      },
    );
    const rendered = `${observed.message}\n${observed.stack}`;
    assert.equal(rendered.includes(secretPath), false);
    assert.equal(rendered.includes(collaboratorMarker), false);
  }
  assert.deepEqual(
    lifecycle.calls,
    methods.map((method) => ({ input: Object.freeze({ method }), method })),
  );
});

test("capture composes the real coordinator, publication, journal, and catalogue", async (t) => {
  let fixture;
  let authorizedBeforePublication;
  fixture = await createFixture(t, {
    onCaptureAttemptAuthorized(attempt) {
      assert.equal(fixture.observation.captureGuard, true);
      assert.equal(
        fixture.observation.events.includes("publication:prepared"),
        false,
      );
      authorizedBeforePublication = attempt;
    },
  });
  const capability = await issueCapability(fixture);
  fixture.observation.events.length = 0;

  const options = captureCoreOptions(fixture, capability);
  const result = await captureCleanCheckpoint(options);
  const completion = fixture.mutation.state.captureFinalizations[0];

  assert.deepEqual(result, fixedResult(checkpoint(), options.request));
  assert(Object.isFrozen(result));
  assert.equal(fixture.stopCalls, 1);
  assert.equal(fixture.resolverCalls, 1);
  assert.equal(fixture.mutation.state.captureRuns, 1);
  assert.strictEqual(
    fixture.mutation.state.captureAttempts.get(CAPTURE_OPERATION_ID).binding,
    authorizedBeforePublication.binding,
  );
  assert.equal(
    fixture.mutation.state.captureAttempts.get(CAPTURE_OPERATION_ID).state,
    "committed",
  );
  assert.equal(
    fixture.mutation.state.captureCatalogue.has(CAPTURE_OPERATION_ID),
    true,
  );
  assert.strictEqual(completion, fixture.mutation.state.captureCallbackCompletions[0]);
  assert.equal(completion.replayed, false);
  assert.deepEqual(completion.artifactProof, {
    artifactManifestDigest: completion.materialization.artifactManifestDigest,
    captureOperationId: CAPTURE_OPERATION_ID,
    modeledDigest: completion.materialization.modeledDigest,
  });
  assert.strictEqual(fixture.artifactProof, completion.artifactProof);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.deepEqual(fixture.observation.events, [
    "resolver",
    "authority:capture:start",
    "authority:capture:attempt-authorized",
    "publication:prepared",
    "publication:copied",
    "authority:capture:finalized",
    "authority:capture:end",
  ]);

  const serializedAdmission = JSON.stringify(fixture.mutation.state.captureAdmissions[0]);
  const serializedCompletion = JSON.stringify(completion);
  for (const forbidden of [
    fixture.sourceDirectory,
    fixture.artifactDirectory,
    fixture.writerAttachment.rootPath,
  ]) {
    assert.equal(serializedCompletion.includes(forbidden), false);
  }
  for (const forbidden of [
    "stoppedWriterEvidence",
    "capability",
    "Bearer ",
  ]) {
    assert.equal(serializedAdmission.includes(forbidden), false);
    assert.equal(serializedCompletion.includes(forbidden), false);
  }
});

test("committed capture reconciliation finalizes an authenticated durable attempt idempotently", async (t) => {
  const fixture = await createFixture(t, { captureFinalizationFailure: true });
  const capability = await issueCapability(fixture);

  await assert.rejects(
    () => captureCleanCheckpoint(captureCoreOptions(fixture, capability)),
    (error) => assertCoreError(error, "checkpoint_outcome_uncertain"),
  );

  const authorized = fixture.mutation.state.captureAttempts.get(
    CAPTURE_OPERATION_ID,
  );
  assert(authorized);
  assert.equal(authorized.state, "authorized");
  assert.equal(authorized.captureAttemptId, CAPTURE_ATTEMPT_ID);
  assert.equal(authorized.binding.captureAttemptId, CAPTURE_ATTEMPT_ID);
  assert.equal(authorized.binding.contractVersion, 2);
  assert.equal(
    fixture.mutation.state.captureCatalogue.has(CAPTURE_OPERATION_ID),
    false,
  );
  const journalBefore = await fixture.journal.read({
    operationId: CAPTURE_OPERATION_ID,
  });
  assert.equal(journalBefore.record.state, "committed");
  assert.equal(
    journalBefore.record.binding.coordinator.captureAttemptId,
    CAPTURE_ATTEMPT_ID,
  );
  assert.deepEqual(journalBefore.record.binding.coordinator, authorized.binding);
  const artifactBefore = await lstat(fixture.artifactDirectory, { bigint: true });
  const stopCalls = fixture.stopCalls;
  const resolverCalls = fixture.resolverCalls;
  fixture.observation.events.length = 0;

  const input = captureReconciliationInput(fixture);
  const result = await reconcileCleanCheckpointCapture({
    backend: fixture.backend,
    checkpoint: input.checkpoint,
    manifest: manifest(),
    request: input.request,
    storageRef: storageRef(),
  });
  const completion = fixture.mutation.state.reconciliationFinalizations[0];
  assert.deepEqual(result, fixedResult(input.checkpoint, input.request));
  assert.equal(completion.replayed, true);
  assert.deepEqual(completion.artifactProof, {
    artifactManifestDigest: completion.materialization.artifactManifestDigest,
    captureOperationId: CAPTURE_OPERATION_ID,
    modeledDigest: completion.materialization.modeledDigest,
  });
  assert.equal(
    fixture.mutation.state.captureAttempts.get(CAPTURE_OPERATION_ID).state,
    "committed",
  );
  assert.equal(
    fixture.mutation.state.captureCatalogue.has(CAPTURE_OPERATION_ID),
    true,
  );
  assert.equal(fixture.stopCalls, stopCalls);
  assert.equal(fixture.resolverCalls, resolverCalls);
  assert.deepEqual(fixture.observation.events, [
    "authority:reconciliation:start",
    "authority:reconciliation:finalized",
    "authority:reconciliation:end",
  ]);

  fixture.observation.events.length = 0;
  const replayed = await reconcileCleanCheckpointCapture({
    backend: fixture.backend,
    checkpoint: input.checkpoint,
    manifest: manifest(),
    request: input.request,
    storageRef: storageRef(),
  });
  assert.deepEqual(replayed, result);
  assert.equal(fixture.mutation.state.reconciliationRuns, 2);
  assert.equal(fixture.mutation.state.reconciliationFinalizations.length, 2);
  const artifactAfter = await lstat(fixture.artifactDirectory, { bigint: true });
  assert.equal(artifactAfter.dev, artifactBefore.dev);
  assert.equal(artifactAfter.ino, artifactBefore.ino);
  assert.equal(artifactAfter.birthtimeNs, artifactBefore.birthtimeNs);
  assert.equal(fixture.stopCalls, stopCalls);
  assert.equal(fixture.resolverCalls, resolverCalls);
  assert.deepEqual(fixture.observation.events, [
    "authority:reconciliation:start",
    "authority:reconciliation:finalized",
    "authority:reconciliation:end",
  ]);
});

test("two overlapping exact reconciliations serialize one durable catalogue result", async (t) => {
  const bothAuthoritiesEntered = deferred();
  const releaseAuthorities = deferred();
  let authorityEntries = 0;
  const fixture = await createFixture(t, {
    captureFinalizationFailure: true,
    reconciliationEntryGate: releaseAuthorities.promise,
    onReconciliationAuthorityEntered() {
      authorityEntries += 1;
      if (authorityEntries === 2) bothAuthoritiesEntered.resolve();
    },
  });
  const capability = await issueCapability(fixture);
  await assert.rejects(
    () => captureCleanCheckpoint(captureCoreOptions(fixture, capability)),
    (error) => assertCoreError(error, "checkpoint_outcome_uncertain"),
  );
  const artifactBefore = await lstat(fixture.artifactDirectory, { bigint: true });
  const stopCalls = fixture.stopCalls;
  const resolverCalls = fixture.resolverCalls;
  const input = captureReconciliationInput(fixture);
  const reconcile = () =>
    reconcileCleanCheckpointCapture({
      backend: fixture.backend,
      checkpoint: input.checkpoint,
      manifest: manifest(),
      request: input.request,
      storageRef: storageRef(),
    });

  const pending = [reconcile(), reconcile()];
  let entryFailure;
  let timeoutId;
  try {
    await Promise.race([
      bothAuthoritiesEntered.promise,
      new Promise((_, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error("two reconciliation authorities did not enter")),
          2_000,
        );
      }),
    ]);
  } catch (error) {
    entryFailure = error;
  } finally {
    clearTimeout(timeoutId);
    releaseAuthorities.resolve();
  }
  if (entryFailure !== undefined) {
    await Promise.allSettled(pending);
    throw entryFailure;
  }
  assert.equal(authorityEntries, 2);
  assert.equal(fixture.mutation.state.reconciliationRuns, 2);
  assert.equal(fixture.mutation.state.reconciliationContexts.length, 0);

  const [first, second] = await Promise.all(pending);
  assert.deepEqual(first, second);
  assert.deepEqual(first, fixedResult(input.checkpoint, input.request));
  assert.equal(
    fixture.mutation.state.captureAttempts.get(CAPTURE_OPERATION_ID).state,
    "committed",
  );
  assert.equal(fixture.mutation.state.captureCatalogue.size, 1);
  const catalogueRecord = fixture.mutation.state.captureCatalogue.get(
    CAPTURE_OPERATION_ID,
  );
  assert(catalogueRecord);
  assert.deepEqual(
    catalogueRecord,
    durableCaptureCompletion(
      fixture.mutation.state.reconciliationFinalizations[0],
    ),
  );
  assert.deepEqual(
    durableCaptureCompletion(
      fixture.mutation.state.reconciliationFinalizations[1],
    ),
    catalogueRecord,
  );
  assert.equal(fixture.mutation.state.reconciliationFinalizations.length, 2);
  assert.equal(fixture.mutation.state.maxActiveReconciliations, 1);
  assert.equal(fixture.mutation.state.activeReconciliations, 0);
  assert.equal(fixture.mutation.state.reconciliationTails.size, 0);
  assert.equal(fixture.stopCalls, stopCalls);
  assert.equal(fixture.resolverCalls, resolverCalls);
  assert.equal(fixture.mutation.state.captureRuns, 1);
  const artifactAfter = await lstat(fixture.artifactDirectory, { bigint: true });
  assert.equal(artifactAfter.dev, artifactBefore.dev);
  assert.equal(artifactAfter.ino, artifactBefore.ino);
  assert.equal(artifactAfter.birthtimeNs, artifactBefore.birthtimeNs);
});

test("capture reconciliation rejects missing, forged, and mismatched durable attempts", async (t) => {
  const scenarios = [
    {
      name: "missing attempt",
      mutate(state) {
        state.captureAttempts.delete(CAPTURE_OPERATION_ID);
      },
    },
    {
      name: "foreign attempt binding",
      mutate(state) {
        const current = state.captureAttempts.get(CAPTURE_OPERATION_ID);
        const captureAttemptId = "capture-attempt-foreign";
        state.captureAttempts.set(
          CAPTURE_OPERATION_ID,
          Object.freeze({
            ...current,
            binding: Object.freeze({
              ...current.binding,
              captureAttemptId,
            }),
            captureAttemptId,
          }),
        );
      },
    },
    {
      name: "request mismatch",
      mutate(state) {
        const current = state.captureAttempts.get(CAPTURE_OPERATION_ID);
        const request = mutationRequest("checkpoint", lease(), {
          holderId: "host-conflicting",
        });
        state.captureAttempts.set(
          CAPTURE_OPERATION_ID,
          Object.freeze({
            ...current,
            request,
            result: fixedResult(current.result.checkpoint, request),
          }),
        );
      },
    },
    {
      name: "result mismatch",
      mutate(state) {
        const current = state.captureAttempts.get(CAPTURE_OPERATION_ID);
        state.captureAttempts.set(
          CAPTURE_OPERATION_ID,
          Object.freeze({
            ...current,
            result: Object.freeze({
              checkpoint: current.result.checkpoint,
              mutation: Object.freeze({
                ...current.result.mutation,
                proofId: "proof-checkpoint-conflicting",
              }),
            }),
          }),
        );
      },
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async (t) => {
      const fixture = await createFixture(t, {
        captureFinalizationFailure: true,
      });
      const capability = await issueCapability(fixture);
      await assert.rejects(
        () => captureCleanCheckpoint(captureCoreOptions(fixture, capability)),
        (error) => assertCoreError(error, "checkpoint_outcome_uncertain"),
      );
      scenario.mutate(fixture.mutation.state);
      const journalBefore = await fixture.journal.read({
        operationId: CAPTURE_OPERATION_ID,
      });
      const artifactBefore = await lstat(fixture.artifactDirectory, {
        bigint: true,
      });
      const stopCalls = fixture.stopCalls;
      const resolverCalls = fixture.resolverCalls;

      await assert.rejects(
        () =>
          fixture.backend.reconcileCheckpointCapture(
            captureReconciliationInput(fixture),
          ),
        assertBackendError,
      );

      assert.equal(fixture.stopCalls, stopCalls);
      assert.equal(fixture.resolverCalls, resolverCalls);
      assert.equal(
        fixture.mutation.state.reconciliationFinalizations.length,
        0,
      );
      assert.deepEqual(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID }))
          .record,
        journalBefore.record,
      );
      const artifactAfter = await lstat(fixture.artifactDirectory, {
        bigint: true,
      });
      assert.equal(artifactAfter.dev, artifactBefore.dev);
      assert.equal(artifactAfter.ino, artifactBefore.ino);
      assert.equal(artifactAfter.birthtimeNs, artifactBefore.birthtimeNs);
    });
  }
});

test("capture reconciliation authority must await exactly one callback and return it by reference", async (t) => {
  for (const mode of ["zero", "multiple", "late-unobserved", "substituted"]) {
    await t.test(mode, async (t) => {
      const lateVerifyIssued = deferred();
      const fixture = await createFixture(t, {
        captureFinalizationFailure: true,
        reconciliationMode: mode,
        onReconciliationLateVerifyIssued: lateVerifyIssued.resolve,
      });
      const capability = await issueCapability(fixture);
      await assert.rejects(
        () => captureCleanCheckpoint(captureCoreOptions(fixture, capability)),
        (error) => assertCoreError(error, "checkpoint_outcome_uncertain"),
      );
      const stopCalls = fixture.stopCalls;
      const resolverCalls = fixture.resolverCalls;

      await assert.rejects(
        () =>
          fixture.backend.reconcileCheckpointCapture(
            captureReconciliationInput(fixture),
          ),
        assertBackendError,
      );
      if (mode === "late-unobserved") await lateVerifyIssued.promise;
      await Promise.all(fixture.mutation.state.background);

      assert.equal(fixture.stopCalls, stopCalls);
      assert.equal(fixture.resolverCalls, resolverCalls);
      assert.equal(
        fixture.mutation.state.captureAttempts.get(CAPTURE_OPERATION_ID).state,
        "authorized",
      );
      assert.equal(
        fixture.mutation.state.reconciliationFinalizations.length,
        0,
      );
      assert.equal(
        fixture.mutation.state.captureCatalogue.has(CAPTURE_OPERATION_ID),
        false,
      );
    });
  }
});

test("restore requires a newer current fence, trusted proof, and detached destination", async (t) => {
  const fixture = await createFixture(t);
  const capability = await issueCapability(fixture);
  await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
  fixture.observation.events.length = 0;

  const options = restoreCoreOptions(fixture);
  const result = await restoreCleanCheckpoint(options);
  const completion = fixture.mutation.state.restoreFinalizations[0];

  assert.deepEqual(result, fixedResult(checkpoint(), options.request));
  assert.equal(fixture.mutation.state.restoreRuns, 1);
  assert.strictEqual(completion, fixture.mutation.state.restoreCallbackCompletions[0]);
  assert.equal(completion.replayed, false);
  assert.equal(await pathExists(fixture.destinationDirectory), true);
  assert.equal(
    fixture.mutation.state.restoreContexts[0].destinationState,
    "detached",
  );
  assert.strictEqual(
    fixture.mutation.state.restoreContexts[0].artifactProof,
    fixture.artifactProof,
  );
  assert.equal(
    BigInt(fixture.mutation.state.restoreContexts[0].canonicalLease.fencingEpoch) >
      BigInt(checkpoint().sourceFencingEpoch),
    true,
  );
  assert.deepEqual(fixture.observation.events, [
    "authority:restore:start",
    "publication:prepared",
    "publication:copied",
    "authority:restore:finalized",
    "authority:restore:end",
  ]);

  const destinationBeforeReplay = await lstat(fixture.destinationDirectory, {
    bigint: true,
  });
  fixture.observation.events.length = 0;
  const replay = await restoreCleanCheckpoint(options);
  const replayCompletion = fixture.mutation.state.restoreFinalizations[1];
  const destinationAfterReplay = await lstat(fixture.destinationDirectory, {
    bigint: true,
  });

  assert.deepEqual(replay, result);
  assert.equal(fixture.mutation.state.restoreRuns, 2);
  assert.equal(fixture.mutation.state.restoreFinalizations.length, 2);
  assert.equal(replayCompletion.replayed, true);
  assert.equal(destinationAfterReplay.dev, destinationBeforeReplay.dev);
  assert.equal(destinationAfterReplay.ino, destinationBeforeReplay.ino);
  assert.equal(
    destinationAfterReplay.birthtimeNs,
    destinationBeforeReplay.birthtimeNs,
  );
  assert.deepEqual(fixture.observation.events, [
    "authority:restore:start",
    "authority:restore:finalized",
    "authority:restore:end",
  ]);
});

test("restore rejects stale authority, attached destinations, and untrusted proofs", async (t) => {
  const cases = [
    {
      name: "stale canonical fence",
      restoreContext: () => ({ canonicalLease: lease() }),
    },
    {
      name: "attached destination",
      restoreContext: () => ({ destinationState: "attached" }),
    },
    {
      name: "untrusted artifact proof",
      restoreContext: (admission) => ({
        artifactProof: {
          artifactManifestDigest: "f".repeat(64),
          captureOperationId: admission.request.operationId,
          modeledDigest: "e".repeat(64),
        },
      }),
    },
    {
      name: "authority destination storage mismatch",
      assertNoPublication: true,
      restoreContext: () => ({
        storageRef: storageRef({ storageId: "volume-substituted" }),
      }),
    },
  ];

  for (const entry of cases) {
    await t.test(entry.name, async (t) => {
      const fixture = await createFixture(t, { restoreContext: entry.restoreContext });
      const capability = await issueCapability(fixture);
      await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
      fixture.observation.events.length = 0;

      await assert.rejects(
        () => restoreCleanCheckpoint(restoreCoreOptions(fixture)),
        (error) => assertCoreError(error, "restore_outcome_uncertain"),
      );
      assert.equal(await pathExists(fixture.destinationDirectory), false);
      assert.equal(fixture.mutation.state.restoreFinalizations.length, 0);
      if (entry.assertNoPublication) {
        assert.equal(
          fixture.observation.events.includes("publication:prepared"),
          false,
        );
      }
    });
  }
});

test("stale or expired capture authority is terminal and never publishes", async (t) => {
  const cases = [
    {
      name: "stale fence",
      captureContext: () => ({
        canonicalLease: restoreLease(),
      }),
    },
    {
      name: "expired lease",
      captureContext: () => ({
        now: Date.parse(lease().expiresAt) + 1,
      }),
    },
  ];

  for (const entry of cases) {
    await t.test(entry.name, async (t) => {
      const fixture = await createFixture(t, { captureContext: entry.captureContext });
      const capability = await issueCapability(fixture);
      const input = captureDispatchInput(fixture, capability);

      await assert.rejects(
        () => fixture.backend.captureCheckpoint(input),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
      assert.equal(fixture.mutation.state.captureCallbackCompletions.length, 0);
      assert.equal(await pathExists(fixture.artifactDirectory), false);

      await assert.rejects(
        () => fixture.backend.captureCheckpoint(input),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
      assert.equal(await pathExists(fixture.artifactDirectory), false);
    });
  }
});

test("one capability admits at most one concurrent capture", async (t) => {
  const entered = deferred();
  const release = deferred();
  const fixture = await createFixture(t, {
    captureGate: release.promise,
    onCaptureEntered: entered.resolve,
  });
  const capability = await issueCapability(fixture);
  const options = captureCoreOptions(fixture, capability);

  const first = captureCleanCheckpoint(options);
  await entered.promise;
  const second = captureCleanCheckpoint(options);
  release.resolve();
  const outcomes = await Promise.allSettled([first, second]);

  assert.equal(outcomes.filter(({ status }) => status === "fulfilled").length, 1);
  const rejected = outcomes.find(({ status }) => status === "rejected");
  assertCoreError(rejected.reason, "checkpoint_outcome_uncertain");
  assert.equal(fixture.mutation.state.captureRuns, 1);
  assert.equal(fixture.mutation.state.captureCallbackCompletions.length, 1);
});

test("authority callback cardinality and awaited identity fail closed", async (t) => {
  for (const captureMode of ["zero", "multiple", "early", "substituted"]) {
    await t.test(captureMode, async (t) => {
      const fixture = await createFixture(t, { captureMode });
      const capability = await issueCapability(fixture);

      await assert.rejects(
        () => fixture.backend.captureCheckpoint(captureDispatchInput(fixture, capability)),
        assertBackendError,
      );
      await Promise.all(fixture.mutation.state.background);
      assert.equal(fixture.mutation.state.captureRuns, 1);

      await assert.rejects(
        () => fixture.backend.captureCheckpoint(captureDispatchInput(fixture, capability)),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
    });
  }
});

test("restore authority callback cardinality and awaited identity fail closed", async (t) => {
  for (const restoreMode of ["zero", "multiple", "early", "substituted"]) {
    await t.test(restoreMode, async (t) => {
      const fixture = await createFixture(t, { restoreMode });
      const capability = await issueCapability(fixture);
      await captureCleanCheckpoint(captureCoreOptions(fixture, capability));

      await assert.rejects(
        () => fixture.backend.restoreCheckpoint(restoreDispatchInput(fixture)),
        assertBackendError,
      );
      await Promise.all(fixture.mutation.state.background);
      assert.equal(fixture.mutation.state.restoreRuns, 1);
      assert.equal(fixture.mutation.state.restoreFinalizations.length, 0);
    });
  }
});

test("backend observes discarded callback rejections", async (t) => {
  const unhandledRejections = [];
  const onUnhandledRejection = (reason, promise) => {
    unhandledRejections.push({ promise, reason });
  };
  process.prependListener("unhandledRejection", onUnhandledRejection);
  try {
    await t.test("capture discarded before authority return", async (t) => {
      const fixture = await createFixture(t, {
        captureMode: "discard-invalid",
      });
      const capability = await issueCapability(fixture);

      await assert.rejects(
        () =>
          fixture.backend.captureCheckpoint(
            captureDispatchInput(fixture, capability),
          ),
        assertBackendError,
      );
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setImmediate(resolve));

      assert.deepEqual(unhandledRejections, []);
      assert.equal(
        fixture.observation.events.includes("publication:prepared"),
        false,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
    });

    await t.test("restore discarded before authority throw", async (t) => {
      const fixture = await createFixture(t, {
        restoreMode: "discard-invalid-throw",
      });
      const capability = await issueCapability(fixture);
      await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
      const publicationEventsBeforeRestore = fixture.observation.events.filter(
        (event) => event.startsWith("publication:"),
      ).length;

      await assert.rejects(
        () =>
          fixture.backend.restoreCheckpoint(restoreDispatchInput(fixture)),
        assertBackendError,
      );
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setImmediate(resolve));

      assert.deepEqual(unhandledRejections, []);
      assert.equal(
        fixture.observation.events.filter((event) =>
          event.startsWith("publication:"),
        ).length,
        publicationEventsBeforeRestore,
      );
      assert.equal(fixture.mutation.state.restoreRuns, 1);
    });

    await t.test("capture invoked after authority return", async (t) => {
      const issued = deferred();
      const fixture = await createFixture(t, {
        captureMode: "late-unobserved",
        onCaptureLatePublishIssued: issued.resolve,
      });
      const capability = await issueCapability(fixture);
      const input = captureDispatchInput(fixture, capability);

      await assert.rejects(
        () => fixture.backend.captureCheckpoint(input),
        assertBackendError,
      );
      await issued.promise;
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setImmediate(resolve));

      assert.deepEqual(unhandledRejections, []);
      assert.equal(
        fixture.observation.events.includes("publication:prepared"),
        false,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);

      await assert.rejects(
        () => fixture.backend.captureCheckpoint(input),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
    });

    await t.test("restore invoked after authority return", async (t) => {
      const issued = deferred();
      const fixture = await createFixture(t, {
        onRestoreLatePublishIssued: issued.resolve,
        restoreMode: "late-unobserved",
      });
      const capability = await issueCapability(fixture);
      await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
      const publicationEventsBeforeRestore = fixture.observation.events.filter(
        (event) => event.startsWith("publication:"),
      ).length;

      await assert.rejects(
        () =>
          fixture.backend.restoreCheckpoint(restoreDispatchInput(fixture)),
        assertBackendError,
      );
      await issued.promise;
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setImmediate(resolve));

      assert.deepEqual(unhandledRejections, []);
      assert.equal(
        fixture.observation.events.filter((event) =>
          event.startsWith("publication:"),
        ).length,
        publicationEventsBeforeRestore,
      );
      assert.equal(fixture.mutation.state.restoreRuns, 1);
    });
  } finally {
    process.removeListener("unhandledRejection", onUnhandledRejection);
  }
});

test("capture authority awaits only Promise values with a safe constructor path", async (t) => {
  const observation = {
    constructorReads: 0,
    prototypeTraps: 0,
    thenCalls: 0,
  };
  for (const scenario of adversarialAuthorityPromiseScenarios(observation)) {
    await t.test(scenario.name, async (t) => {
      const fixture = await createFixture(t, {
        captureReturnFactory: scenario.create,
      });
      const capability = await issueCapability(fixture);

      await assert.rejects(
        () =>
          fixture.backend.captureCheckpoint(
            captureDispatchInput(fixture, capability),
          ),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
      assert.equal(
        fixture.mutation.state.captureCallbackCompletions.length,
        0,
      );
      assert.equal(fixture.mutation.state.captureFinalizations.length, 0);
      assert.equal(await pathExists(fixture.artifactDirectory), false);
      assert.equal(
        fixture.observation.events.includes("publication:prepared"),
        false,
      );
      await assert.rejects(
        () =>
          fixture.backend.captureCheckpoint(
            captureDispatchInput(fixture, capability),
          ),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.captureRuns, 1);
    });
  }
  assert.equal(observation.constructorReads, 0);
  assert.equal(observation.prototypeTraps, 0);
  assert.equal(observation.thenCalls, 0);
});

test("restore authority awaits only Promise values with a safe constructor path", async (t) => {
  const observation = {
    constructorReads: 0,
    prototypeTraps: 0,
    thenCalls: 0,
  };
  for (const scenario of adversarialAuthorityPromiseScenarios(observation)) {
    await t.test(scenario.name, async (t) => {
      const fixture = await createFixture(t, {
        restoreReturnFactory: scenario.create,
      });
      const capability = await issueCapability(fixture);
      await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
      fixture.observation.events.length = 0;

      await assert.rejects(
        () => fixture.backend.restoreCheckpoint(restoreDispatchInput(fixture)),
        assertBackendError,
      );
      assert.equal(fixture.mutation.state.restoreRuns, 1);
      assert.equal(
        fixture.mutation.state.restoreCallbackCompletions.length,
        0,
      );
      assert.equal(fixture.mutation.state.restoreFinalizations.length, 0);
      assert.equal(await pathExists(fixture.destinationDirectory), false);
      assert.equal(
        fixture.observation.events.includes("publication:prepared"),
        false,
      );
    });
  }
  assert.equal(observation.constructorReads, 0);
  assert.equal(observation.prototypeTraps, 0);
  assert.equal(observation.thenCalls, 0);
});

test("capture waits for durable authority finalization after publication", async (t) => {
  const published = deferred();
  const releaseFinalization = deferred();
  const fixture = await createFixture(t, {
    captureFinalizationGate: releaseFinalization.promise,
    onCapturePublished: published.resolve,
  });
  const capability = await issueCapability(fixture);
  const options = captureCoreOptions(fixture, capability);
  let settled = false;
  const pending = captureCleanCheckpoint(options);
  void pending.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );

  const callbackCompletion = await published.promise;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  assert.equal(fixture.observation.captureGuard, true);
  assert.equal(fixture.mutation.state.captureFinalizations.length, 0);
  assert.strictEqual(
    fixture.mutation.state.captureCallbackCompletions[0],
    callbackCompletion,
  );
  await assert.rejects(
    () =>
      fixture.backend.captureCheckpoint(
        captureDispatchInput(fixture, capability),
      ),
    assertBackendError,
  );
  assert.equal(fixture.mutation.state.captureRuns, 1);
  assert.equal(settled, false);

  releaseFinalization.resolve();
  const result = await pending;
  assert.deepEqual(result, fixedResult(checkpoint(), options.request));
  assert.equal(settled, true);
  assert.equal(fixture.observation.captureGuard, false);
  assert.equal(fixture.mutation.state.captureFinalizations.length, 1);
  assert.strictEqual(
    fixture.mutation.state.captureFinalizations[0],
    callbackCompletion,
  );
});

test("restore waits for durable authority finalization after publication", async (t) => {
  const published = deferred();
  const releaseFinalization = deferred();
  const fixture = await createFixture(t, {
    onRestorePublished: published.resolve,
    restoreFinalizationGate: releaseFinalization.promise,
  });
  const capability = await issueCapability(fixture);
  await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
  const options = restoreCoreOptions(fixture);
  let settled = false;
  const pending = restoreCleanCheckpoint(options);
  void pending.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );

  const callbackCompletion = await published.promise;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  assert.equal(fixture.observation.restoreGuard, true);
  assert.equal(fixture.mutation.state.restoreFinalizations.length, 0);
  assert.strictEqual(
    fixture.mutation.state.restoreCallbackCompletions[0],
    callbackCompletion,
  );

  releaseFinalization.resolve();
  const result = await pending;
  assert.deepEqual(result, fixedResult(checkpoint(), options.request));
  assert.equal(settled, true);
  assert.equal(fixture.observation.restoreGuard, false);
  assert.equal(fixture.mutation.state.restoreFinalizations.length, 1);
  assert.strictEqual(
    fixture.mutation.state.restoreFinalizations[0],
    callbackCompletion,
  );
});

test("constructor rejects an async stopped-writer resolver before invocation", async (t) => {
  let resolverCalls = 0;
  await assert.rejects(
    () =>
      createFixture(t, {
        async resolveStoppedWriter() {
          resolverCalls += 1;
          return undefined;
        },
      }),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
  assert.equal(resolverCalls, 0);
});

test("constructor rejects opaque native and bound stopped-writer resolvers", async (t) => {
  const calls = {
    arrow: 0,
    async: 0,
    asyncGenerator: 0,
    generator: 0,
    sync: 0,
  };
  function syncResolver() {
    calls.sync += 1;
  }
  const arrowResolver = () => {
    calls.arrow += 1;
  };
  async function asyncResolver() {
    calls.async += 1;
  }
  function* generatorResolver() {
    calls.generator += 1;
  }
  async function* asyncGeneratorResolver() {
    calls.asyncGenerator += 1;
  }
  const scenarios = [
    ["bound sync", syncResolver.bind(undefined)],
    ["bound arrow", arrowResolver.bind(undefined)],
    ["bound async", asyncResolver.bind(undefined)],
    ["bound generator", generatorResolver.bind(undefined)],
    ["bound async generator", asyncGeneratorResolver.bind(undefined)],
    ["native builtin", Math.max],
  ];

  for (const [name, resolveStoppedWriter] of scenarios) {
    await t.test(name, async (t) => {
      await assert.rejects(
        () => createFixture(t, { resolveStoppedWriter }),
        (error) =>
          assertBackendError(
            error,
            "invalid_stopped_directory_backend_request",
          ),
      );
    });
  }
  assert.deepEqual(calls, {
    arrow: 0,
    async: 0,
    asyncGenerator: 0,
    generator: 0,
    sync: 0,
  });
});

test("constructor rejects a disguised bound async resolver without metadata access", async (t) => {
  let bodyCalls = 0;
  let getterReads = 0;
  async function asyncResolver() {
    bodyCalls += 1;
  }
  const resolveStoppedWriter = asyncResolver.bind(undefined);
  Object.setPrototypeOf(resolveStoppedWriter, Function.prototype);
  for (const key of ["constructor", "name", "toString", Symbol.toStringTag]) {
    Object.defineProperty(resolveStoppedWriter, key, {
      configurable: true,
      get() {
        getterReads += 1;
        throw new Error("resolver metadata must remain opaque");
      },
    });
  }

  await assert.rejects(
    () => createFixture(t, { resolveStoppedWriter }),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
  assert.equal(bodyCalls, 0);
  assert.equal(getterReads, 0);
});

test("constructor rejects a bound proxied async resolver without target access", async (t) => {
  let bodyCalls = 0;
  let targetTraps = 0;
  const target = new Proxy(
    async function asyncResolver() {
      bodyCalls += 1;
    },
    {
      apply(operation, thisValue, args) {
        targetTraps += 1;
        return Reflect.apply(operation, thisValue, args);
      },
      get(operation, key, receiver) {
        targetTraps += 1;
        return Reflect.get(operation, key, receiver);
      },
    },
  );
  const resolveStoppedWriter = Reflect.apply(
    Function.prototype.bind,
    target,
    [undefined],
  );
  targetTraps = 0;

  await assert.rejects(
    () => createFixture(t, { resolveStoppedWriter }),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
  assert.equal(bodyCalls, 0);
  assert.equal(targetTraps, 0);
});

test("source-backed resolvers may contain native-code text", async (t) => {
  const fixture = await createFixture(t, {
    resolveStoppedWriterFactory: ({ resolveDefault }) =>
      function resolveStoppedWriter(input) {
        const diagnosticText = "[native code]";
        assert.equal(diagnosticText.length, 13);
        return resolveDefault(input);
      },
  });
  const capability = await issueCapability(fixture);
  const options = captureCoreOptions(fixture, capability);

  const result = await captureCleanCheckpoint(options);

  assert.deepEqual(result, fixedResult(checkpoint(), options.request));
  assert.equal(fixture.resolverCalls, 1);
});

test("a rejected native resolver Promise is observed without consuming authority", async (t) => {
  const unhandledRejections = [];
  const onUnhandledRejection = (reason, promise) => {
    unhandledRejections.push({ promise, reason });
  };
  process.prependListener("unhandledRejection", onUnhandledRejection);
  try {
    const resolverFailure = new Error(
      "resolver secret at /company/private/resolver-state",
    );
    let resolverInvocations = 0;
    let rejectedResolution;
    const fixture = await createFixture(t, {
      resolveStoppedWriterFactory: ({ resolveDefault }) => (input) => {
        resolverInvocations += 1;
        if (resolverInvocations === 1) {
          rejectedResolution = Promise.reject(resolverFailure);
          return rejectedResolution;
        }
        return resolveDefault(input);
      },
    });
    const capability = await issueCapability(fixture);
    let observedError;

    await assert.rejects(
      () =>
        fixture.backend.captureCheckpoint(
          captureDispatchInput(fixture, capability),
        ),
      (error) => {
        observedError = error;
        return assertBackendError(error);
      },
    );
    assert(rejectedResolution instanceof Promise);
    assert.equal(resolverInvocations, 1);
    assert.equal(fixture.mutation.state.captureRuns, 0);
    assert.equal(await pathExists(fixture.artifactDirectory), false);
    assert.equal(
      `${observedError.message}\n${observedError.stack}`.includes(
        "/company/private/resolver-state",
      ),
      false,
    );

    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(unhandledRejections, []);

    const options = captureCoreOptions(fixture, capability);
    const result = await captureCleanCheckpoint(options);
    assert.deepEqual(result, fixedResult(checkpoint(), options.request));
    assert.equal(resolverInvocations, 2);
    assert.equal(fixture.resolverCalls, 1);
    assert.equal(fixture.mutation.state.captureRuns, 1);
  } finally {
    process.removeListener("unhandledRejection", onUnhandledRejection);
  }
});

test("resolver thenable accessors remain opaque and do not consume authority", async (t) => {
  let resolverInvocations = 0;
  let thenCalls = 0;
  let thenReads = 0;
  const fixture = await createFixture(t, {
    resolveStoppedWriterFactory: ({ resolveDefault }) => (input) => {
      resolverInvocations += 1;
      if (resolverInvocations !== 1) return resolveDefault(input);
      return Object.defineProperty({}, "then", {
        configurable: true,
        enumerable: true,
        get() {
          thenReads += 1;
          return () => {
            thenCalls += 1;
          };
        },
      });
    },
  });
  const capability = await issueCapability(fixture);

  await assert.rejects(
    () =>
      fixture.backend.captureCheckpoint(
        captureDispatchInput(fixture, capability),
      ),
    assertBackendError,
  );
  assert.equal(thenReads, 0);
  assert.equal(thenCalls, 0);
  assert.equal(fixture.mutation.state.captureRuns, 0);
  assert.equal(await pathExists(fixture.artifactDirectory), false);

  const options = captureCoreOptions(fixture, capability);
  const result = await captureCleanCheckpoint(options);
  assert.deepEqual(result, fixedResult(checkpoint(), options.request));
  assert.equal(resolverInvocations, 2);
  assert.equal(thenReads, 0);
  assert.equal(thenCalls, 0);
});

test("deterministic invalid inputs fail before resolver, authority, publication, or journal", async (t) => {
  const fixture = await createFixture(t);
  const capability = await issueCapability(fixture);
  const invalidCapture = {
    ...captureDispatchInput(fixture, capability),
    unexpected: true,
  };
  const invalidRestore = {
    checkpoint: checkpoint(),
    request: mutationRequest("restore", fixture.restoreWriterLease),
    unexpected: true,
  };
  const invalidReconciliation = {
    ...captureReconciliationInput(fixture),
    unexpected: true,
  };

  await assert.rejects(
    () => fixture.backend.captureCheckpoint(invalidCapture),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
  await assert.rejects(
    () => fixture.backend.restoreCheckpoint(invalidRestore),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
  await assert.rejects(
    () => fixture.backend.reconcileCheckpointCapture(invalidReconciliation),
    (error) =>
      assertBackendError(error, "invalid_stopped_directory_backend_request"),
  );
  assert.equal(fixture.resolverCalls, 0);
  assert.equal(fixture.mutation.state.captureRuns, 0);
  assert.equal(fixture.mutation.state.restoreRuns, 0);
  assert.equal(fixture.mutation.state.reconciliationRuns, 0);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(await pathExists(fixture.destinationDirectory), false);

  await captureCleanCheckpoint(captureCoreOptions(fixture, capability));
  assert.equal(fixture.resolverCalls, 1);
  assert.equal(fixture.mutation.state.captureRuns, 1);
});

test("direct restore rejects non-newer epochs before authority admission", async (t) => {
  const fixture = await createFixture(t);

  for (const fencingEpoch of ["11", "10"]) {
    await t.test(`epoch ${fencingEpoch}`, async () => {
      await assert.rejects(
        () =>
          fixture.backend.restoreCheckpoint({
            checkpoint: checkpoint(),
            request: mutationRequest(
              "restore",
              restoreLease({ fencingEpoch }),
            ),
          }),
        (error) =>
          assertBackendError(
            error,
            "invalid_stopped_directory_backend_request",
          ),
      );
    });
  }

  assert.equal(fixture.resolverCalls, 0);
  assert.equal(fixture.mutation.state.captureRuns, 0);
  assert.equal(fixture.mutation.state.restoreRuns, 0);
  assert.equal(await pathExists(fixture.destinationDirectory), false);
  assert.deepEqual(await readdir(fixture.journalDirectory), []);
  assert.equal(
    fixture.observation.events.includes("publication:prepared"),
    false,
  );
});

test("runtime collaborator failures become fixed path-free uncertainty", async (t) => {
  const fixture = await createFixture(t, { captureMode: "throw" });
  const capability = await issueCapability(fixture);
  let observed;

  await assert.rejects(
    () => fixture.backend.captureCheckpoint(captureDispatchInput(fixture, capability)),
    (error) => {
      observed = error;
      return assertBackendError(error);
    },
  );
  const rendered = `${observed.message}\n${observed.stack}`;
  for (const secret of [
    "sensitive-token-value",
    "/company/private/session",
    fixture.sourceDirectory,
    fixture.writerAttachment.rootPath,
  ]) {
    assert.equal(rendered.includes(secret), false);
  }
  assert.equal(fixture.mutation.state.captureRuns, 1);

  await assert.rejects(
    () => fixture.backend.captureCheckpoint(captureDispatchInput(fixture, capability)),
    assertBackendError,
  );
  assert.equal(fixture.mutation.state.captureRuns, 1);
});

test("capture reconciliation never adopts prepared or materialized publication state", async (t) => {
  for (const phase of ["prepared", "materialized"]) {
    await t.test(phase, async (t) => {
      let seedFaultEnabled = true;
      const fixture = await createFixture(t, {
        publicationFaults: {
          async afterJournalPrepared() {
            if (phase === "prepared" && seedFaultEnabled) {
              throw new Error("prepared reconciliation seed fault");
            }
          },
          async afterMaterialized() {
            if (phase === "materialized" && seedFaultEnabled) {
              throw new Error("materialized reconciliation seed fault");
            }
          },
        },
      });
      const publicationOptions = capturePublicationOptions(fixture);
      fixture.observation.preseeding = true;
      try {
        await assert.rejects(
          fixture.publication.publishCheckpointArtifact(publicationOptions),
          (error) =>
            error?.code === "publication_io_failed" &&
            error.commitState === "not-committed",
        );
      } finally {
        fixture.observation.preseeding = false;
      }
      seedFaultEnabled = false;

      const admission = Object.freeze({
        checkpoint: checkpoint(),
        request: mutationRequest("checkpoint", fixture.writerLease),
      });
      fixture.mutation.state.captureAttempts.set(
        CAPTURE_OPERATION_ID,
        captureAttemptRecord(fixture, admission),
      );
      const journalBefore = await fixture.journal.read({
        operationId: CAPTURE_OPERATION_ID,
      });
      assert.equal(journalBefore.record.state, phase);
      const candidate = captureCandidatePath(fixture);
      const candidateBefore =
        phase === "materialized"
          ? await lstat(candidate, { bigint: true })
          : null;
      await rm(fixture.sourceDirectory, { force: true, recursive: true });
      fixture.observation.events.length = 0;

      await assert.rejects(
        () =>
          fixture.backend.reconcileCheckpointCapture(
            captureReconciliationInput(fixture),
          ),
        assertBackendError,
      );

      assert.equal(fixture.stopCalls, 0);
      assert.equal(fixture.resolverCalls, 0);
      assert.equal(fixture.mutation.state.captureRuns, 0);
      assert.equal(fixture.mutation.state.reconciliationRuns, 1);
      assert.equal(
        fixture.mutation.state.reconciliationCallbackCompletions.length,
        0,
      );
      assert.equal(
        fixture.mutation.state.reconciliationFinalizations.length,
        0,
      );
      assert.equal(
        fixture.mutation.state.captureAttempts.get(CAPTURE_OPERATION_ID).state,
        "authorized",
      );
      assert.deepEqual(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID }))
          .record,
        journalBefore.record,
      );
      assert.equal(await pathExists(fixture.artifactDirectory), false);
      if (candidateBefore === null) {
        assert.equal(await pathExists(candidate), false);
      } else {
        const candidateAfter = await lstat(candidate, { bigint: true });
        assert.equal(candidateAfter.dev, candidateBefore.dev);
        assert.equal(candidateAfter.ino, candidateBefore.ino);
        assert.equal(candidateAfter.birthtimeNs, candidateBefore.birthtimeNs);
      }
      assert.deepEqual(fixture.observation.events, [
        "authority:reconciliation:start",
        "authority:reconciliation:end",
      ]);
    });
  }
});

test("pre-existing capture phases cannot satisfy a new stopped-writer capability", async (t) => {
  for (const phase of ["prepared", "materialized", "committed"]) {
    await t.test(phase, async (t) => {
      let seedFaultEnabled = phase !== "committed";
      const fixture = await createFixture(t, {
        publicationFaults: {
          async afterJournalPrepared() {
            if (phase === "prepared" && seedFaultEnabled) {
              throw new Error("prepared capture seed fault");
            }
          },
          async afterMaterialized() {
            if (phase === "materialized" && seedFaultEnabled) {
              throw new Error("materialized capture seed fault");
            }
          },
        },
      });
      const publicationOptions = capturePublicationOptions(fixture);
      fixture.observation.preseeding = true;
      try {
        if (phase === "committed") {
          await fixture.publication.publishCheckpointArtifact(
            publicationOptions,
          );
        } else {
          await assert.rejects(
            fixture.publication.publishCheckpointArtifact(publicationOptions),
            (error) =>
              error?.code === "publication_io_failed" &&
              error.commitState === "not-committed",
          );
        }
      } finally {
        fixture.observation.preseeding = false;
      }
      seedFaultEnabled = false;

      const journalBefore = await fixture.journal.read({
        operationId: CAPTURE_OPERATION_ID,
      });
      assert.equal(journalBefore.record.state, phase);
      const candidate = captureCandidatePath(fixture);
      const durablePath =
        phase === "materialized"
          ? candidate
          : phase === "committed"
            ? fixture.artifactDirectory
            : null;
      const durableIdentity =
        durablePath === null ? null : await lstat(durablePath, { bigint: true });
      if (phase !== "prepared") {
        await rm(fixture.sourceDirectory, { force: true, recursive: true });
      }
      fixture.observation.events.length = 0;

      const capability = await issueCapability(fixture);
      const options = captureCoreOptions(fixture, capability);
      await assert.rejects(
        () => captureCleanCheckpoint(options),
        (error) => assertCoreError(error, "checkpoint_outcome_uncertain"),
      );

      assert.equal(fixture.stopCalls, 1);
      assert.equal(fixture.resolverCalls, 1);
      assert.equal(fixture.mutation.state.captureRuns, 1);
      assert.equal(
        fixture.mutation.state.captureCallbackCompletions.length,
        0,
      );
      assert.equal(fixture.mutation.state.captureFinalizations.length, 0);
      assert.deepEqual(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
        journalBefore.record,
      );
      if (durablePath === null) {
        assert.equal(await pathExists(candidate), false);
        assert.equal(await pathExists(fixture.artifactDirectory), false);
      } else {
        const after = await lstat(durablePath, { bigint: true });
        assert.equal(after.dev, durableIdentity.dev);
        assert.equal(after.ino, durableIdentity.ino);
        assert.equal(after.birthtimeNs, durableIdentity.birthtimeNs);
      }
      assert.deepEqual(fixture.observation.events, [
        "resolver",
        "authority:capture:start",
        "authority:capture:attempt-authorized",
        "authority:capture:end",
      ]);

      await assert.rejects(
        () => captureCleanCheckpoint(options),
        (error) => assertCoreError(error, "checkpoint_outcome_uncertain"),
      );
      assert.equal(fixture.resolverCalls, 2);
      assert.equal(fixture.mutation.state.captureRuns, 1);
      assert.equal(fixture.mutation.state.captureFinalizations.length, 0);
    });
  }
});
