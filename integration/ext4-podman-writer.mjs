import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { execFile } from "node:child_process";
import { lstat, readFile } from "node:fs/promises";
import { resolve as resolvePath } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import {
  PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
  createPodmanWriterSupervisor,
} from "../src/podman-writer-supervisor.mjs";
import {
  createPodmanWriterSupervisorState,
} from "../src/podman-writer-supervisor-state.mjs";

const READY_MARKER = "ready\n";
const SUPERVISOR_ID = "ext4-podman-composition-v1";
const PODMAN_EXECUTION_PATH = "/usr/bin:/bin";
const PODMAN_COMMAND_TIMEOUT_MILLISECONDS = 30_000;
const PODMAN_COMMAND_MAX_OUTPUT_BYTES = 1024 * 1024;
const assertEqualIntrinsic = assert.equal;
const assertNotEqualIntrinsic = assert.notEqual;
const execFileAsync = promisify(execFile);
const pathResolveIntrinsic = resolvePath;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const arrayIsArrayIntrinsic = Array.isArray;
const bufferConstructorIntrinsic = Buffer;
const bufferFromIntrinsic = Buffer.from;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const jsonParseIntrinsic = JSON.parse;
const objectAssignIntrinsic = Object.assign;
const objectCreateIntrinsic = Object.create;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic =
  Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectPrototypeIntrinsic = Object.prototype;

const ROOTLESS_NAMESPACE_CONFIGURATION_KEYS = Object.freeze([
  "exclusiveRootlessEngine",
  "podmanEnvironment",
  "podmanExecutable",
]);
const PODMAN_ENVIRONMENT_KEYS = Object.freeze([
  "HOME",
  "LANG",
  "XDG_RUNTIME_DIR",
]);
const COMMAND_RESULT_KEYS = Object.freeze(["stderr", "stdout"]);
const CONTAINER_INVENTORY_ARGUMENTS = Object.freeze([
  "--remote=false",
  "ps",
  "--all",
  "--external",
  "--no-trunc",
  "--format=json",
]);
const POD_INVENTORY_ARGUMENTS = Object.freeze([
  "--remote=false",
  "pod",
  "ps",
  "--no-trunc",
  "--format=json",
]);
const MIGRATE_ARGUMENTS = Object.freeze([
  "--remote=false",
  "system",
  "migrate",
]);

function exact(value) {
  return objectFreezeIntrinsic(
    objectAssignIntrinsic(objectCreateIntrinsic(null), value),
  );
}

function exactDataRecord(value, expectedKeys) {
  assertEqualIntrinsic(value !== null && typeof value === "object", true);
  assertEqualIntrinsic(arrayIsArrayIntrinsic(value), false);
  const prototype = objectGetPrototypeOfIntrinsic(value);
  assertEqualIntrinsic(
    prototype === null || prototype === objectPrototypeIntrinsic,
    true,
  );
  const keys = reflectOwnKeysIntrinsic(value);
  assertEqualIntrinsic(keys.length, expectedKeys.length);
  const normalized = objectCreateIntrinsic(null);
  for (let index = 0; index < expectedKeys.length; index += 1) {
    const key = expectedKeys[index];
    const descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
    assertEqualIntrinsic(descriptor !== undefined, true);
    assertEqualIntrinsic(objectHasOwnIntrinsic(descriptor, "value"), true);
    assertEqualIntrinsic(descriptor.enumerable, true);
    normalized[key] = descriptor.value;
  }
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    assertEqualIntrinsic(typeof key, "string");
    let recognized = false;
    for (
      let expectedIndex = 0;
      expectedIndex < expectedKeys.length;
      expectedIndex += 1
    ) {
      if (key === expectedKeys[expectedIndex]) recognized = true;
    }
    assertEqualIntrinsic(recognized, true);
  }
  return objectFreezeIntrinsic(normalized);
}

function canonicalAbsolutePath(value) {
  assertEqualIntrinsic(typeof value, "string");
  assertEqualIntrinsic(value.length > 0 && value.length <= 4_095, true);
  for (let index = 0; index < value.length; index += 1) {
    assertEqualIntrinsic(value[index] !== "\0", true);
    assertEqualIntrinsic(value[index] !== "\r", true);
    assertEqualIntrinsic(value[index] !== "\n", true);
  }
  assertEqualIntrinsic(value[0], "/");
  const encoded = reflectApplyIntrinsic(
    bufferFromIntrinsic,
    bufferConstructorIntrinsic,
    [value, "utf8"],
  );
  assertEqualIntrinsic(encoded.length <= 4_095, true);
  assertEqualIntrinsic(
    reflectApplyIntrinsic(bufferToStringIntrinsic, encoded, ["utf8"]),
    value,
  );
  assertEqualIntrinsic(pathResolveIntrinsic(value), value);
  return value;
}

function commandResultWithEmptyStderr(value) {
  const result = exactDataRecord(value, COMMAND_RESULT_KEYS);
  assertEqualIntrinsic(typeof result.stderr, "string");
  assertEqualIntrinsic(typeof result.stdout, "string");
  assertEqualIntrinsic(result.stderr, "");
  return result;
}

function assertEmptyInventory(value) {
  const result = commandResultWithEmptyStderr(value);
  const inventory = jsonParseIntrinsic(result.stdout);
  assertEqualIntrinsic(arrayIsArrayIntrinsic(inventory), true);
  assertEqualIntrinsic(inventory.length, 0);
}

// This teardown is intentionally narrower than the production supervisor.
// The explicit opt-in asserts that this conformance process owns the complete
// rootless engine. Empty container and pod inventories prove that retiring its
// pause process cannot disrupt another workload before releasing cloned mounts.
export async function retireExt4PodmanRootlessNamespaceForConformance(
  configuration,
  runCommand = execFileAsync,
) {
  const normalizedConfiguration = exactDataRecord(
    configuration,
    ROOTLESS_NAMESPACE_CONFIGURATION_KEYS,
  );
  assertEqualIntrinsic(normalizedConfiguration.exclusiveRootlessEngine, true);
  assertEqualIntrinsic(typeof runCommand, "function");
  const podmanExecutable = canonicalAbsolutePath(
    normalizedConfiguration.podmanExecutable,
  );
  assertNotEqualIntrinsic(podmanExecutable, "/");
  const configuredEnvironment = exactDataRecord(
    normalizedConfiguration.podmanEnvironment,
    PODMAN_ENVIRONMENT_KEYS,
  );
  const home = canonicalAbsolutePath(configuredEnvironment.HOME);
  const runtimeDirectory = canonicalAbsolutePath(
    configuredEnvironment.XDG_RUNTIME_DIR,
  );
  assertEqualIntrinsic(configuredEnvironment.LANG, "C.UTF-8");
  const options = exact({
    cwd: "/",
    encoding: "utf8",
    env: exact({
      HOME: home,
      LANG: "C.UTF-8",
      PATH: PODMAN_EXECUTION_PATH,
      XDG_RUNTIME_DIR: runtimeDirectory,
    }),
    killSignal: "SIGKILL",
    maxBuffer: PODMAN_COMMAND_MAX_OUTPUT_BYTES,
    shell: false,
    timeout: PODMAN_COMMAND_TIMEOUT_MILLISECONDS,
  });

  assertEmptyInventory(await runCommand(
    podmanExecutable,
    CONTAINER_INVENTORY_ARGUMENTS,
    options,
  ));
  assertEmptyInventory(await runCommand(
    podmanExecutable,
    POD_INVENTORY_ARGUMENTS,
    options,
  ));
  const migrated = commandResultWithEmptyStderr(await runCommand(
    podmanExecutable,
    MIGRATE_ARGUMENTS,
    options,
  ));
  assertEqualIntrinsic(migrated.stdout, "");
}

function measuredImage(imageDigest) {
  return exact({
    projection: exact({
      codexSandbox: "workspace-write",
      codexVersion: "1.0.0",
      platformImage: exact({
        architecture: "amd64",
        config: exact({
          digest: `sha256:${"b".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 1,
        }),
        digest: imageDigest,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 1,
      }),
    }),
    runtimeIdentity: exact({
      codexBinaryPath: "/usr/local/bin/writer",
      codexBinarySha256: "c".repeat(64),
      codexVersion: "1.0.0",
      platformImageDigest: imageDigest,
    }),
  });
}

function launchInput(attachment, imageDigest) {
  const lease = exact({
    contractVersion: 1,
    expiresAt: "2030-01-01T00:00:00.000Z",
    fencingEpoch: attachment.fencingEpoch,
    holderId: attachment.holderId,
    leaseId: attachment.leaseId,
    sessionId: attachment.sessionId,
  });
  const generationReference = exact({
    bindingSha256: "1".repeat(64),
    checkpointId: "ext4-podman-checkpoint-001",
    claimedAt: "2026-08-14T09:00:00.000Z",
    committedAt: "2026-08-14T09:01:00.000Z",
    documentSha256: "2".repeat(64),
    generationId: "ext4-podman-generation-001",
    operationId: "ext4-podman-generation-operation-001",
    sessionId: attachment.sessionId,
    state: "committed",
  });
  const request = exact({
    attachment,
    contractVersion: 1,
    fencingEpoch: attachment.fencingEpoch,
    generation: generationReference,
    lease,
    measuredImage: measuredImage(imageDigest),
    supervisor: exact({ contractVersion: 1, supervisorId: SUPERVISOR_ID }),
  });
  const launchAttemptId = "ext4-podman-launch-attempt-001";
  const session = exact({
    createdAt: "2026-08-14T08:00:00.000Z",
    document: exact({
      activeOperation: exact({ operationId: launchAttemptId }),
      attachment,
      backendCapabilities: exact({ exclusiveWriterAttachment: true }),
      documentVersion: 3,
      lastOperation: null,
      launch: null,
      lease,
      lifecycle: "ATTACHED",
      manifest: exact({ runtime: exact({ imageDigest }) }),
      recovery: null,
      storageRef: exact({ storageId: attachment.storageId }),
      writerEpoch: attachment.fencingEpoch,
    }),
    revision: "10",
    sessionId: attachment.sessionId,
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const attempt = exact({
    contractVersion: 1,
    launchAttemptId,
    request,
    result: null,
    state: "starting",
  });
  const operation = exact({
    conflictClass: "session-mutation",
    createdAt: "2026-08-14T09:59:00.000Z",
    expectedSession: session,
    kind: "writer-launch-attempt-v1",
    operationId: launchAttemptId,
    request,
    requestSha256: "d".repeat(64),
    result: null,
    retiredAt: null,
    revision: "1",
    sessionId: attachment.sessionId,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const reservation = exact({
    conflictClass: "session-mutation",
    createdAt: "2026-08-14T09:59:00.000Z",
    expectedSessionRevision: "9",
    expiresAt: null,
    kind: operation.kind,
    operationId: launchAttemptId,
    releasedAt: null,
    requestSha256: operation.requestSha256,
    reservationId: "ext4-podman-reservation-001",
    sessionId: attachment.sessionId,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  return exact({
    attempt,
    authorityNow: "2026-08-14T10:00:01.000Z",
    consumedImage: request.measuredImage,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    generation: exact({
      binding: exact({ provider: "ext4-podman-integration" }),
      checkpointId: generationReference.checkpointId,
      claimedAt: generationReference.claimedAt,
      committedAt: generationReference.committedAt,
      document: exact({ status: "committed" }),
      generationId: generationReference.generationId,
      operationId: generationReference.operationId,
      sessionId: generationReference.sessionId,
      state: "committed",
    }),
    invocation: exact({}),
    operation,
    reservation,
    session,
    signal: new AbortController().signal,
  });
}

function stopInput(input, receipt) {
  return exact({
    attachment: input.attempt.request.attachment,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    invocation: exact({}),
    processIncarnationId: receipt.evidence.processIncarnationId,
    signal: new AbortController().signal,
    stopOperationId: "ext4-podman-stop-operation-001",
    writerFence: exact({
      contractVersion: 1,
      fencingEpoch: input.attempt.request.lease.fencingEpoch,
      holderId: input.attempt.request.lease.holderId,
      leaseId: input.attempt.request.lease.leaseId,
      sessionId: input.attempt.request.lease.sessionId,
    }),
    writerIncarnationId: receipt.evidence.writerIncarnationId,
  });
}

export async function waitForExt4PodmanReadyMarker(path, readMarker = readFile) {
  const deadline = Date.now() + 10_000;
  while (true) {
    let contents;
    let missingError = null;
    try {
      contents = await readMarker(path, "utf8");
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      missingError = error;
    }
    if (contents === READY_MARKER) return;
    if (Date.now() >= deadline) {
      if (missingError !== null) throw missingError;
      assert.equal(contents, READY_MARKER);
    }
    await delay(50);
  }
}

export async function runExt4PodmanWriterIntegration({
  attachment,
  configuredAttachmentRoot,
  filesystemAuthority,
  imageDigest,
  imageReference,
  podmanEnvironment,
  podmanExecutable,
  stateRoot,
}) {
  assert.equal(process.platform, "linux");
  assert.notEqual(process.getuid(), 0);
  assert.match(imageDigest, /^sha256:[0-9a-f]{64}$/u);
  assert.equal(imageReference, `localhost/portable-codex-writer@${imageDigest}`);
  assert.match(podmanExecutable, /^\/(?:[^/\0]+\/)*[^/\0]+$/u);
  assert.match(podmanEnvironment.HOME, /^\//u);
  assert.match(podmanEnvironment.XDG_RUNTIME_DIR, /^\//u);
  const servicePid = process.pid;
  const serviceUid = process.getuid();
  const state = createPodmanWriterSupervisorState(exact({ root: stateRoot }));
  const supervisor = createPodmanWriterSupervisor(exact({
    commandTimeoutMilliseconds: 30_000,
    configuredAttachmentRoot,
    filesystemAuthority,
    images: exact({
      [imageDigest]: exact({
        architecture: "amd64",
        codexVersion: "1.0.0",
        imageReference,
        os: "linux",
      }),
    }),
    maxOutputBytes: 1024 * 1024,
    podmanEnvironment,
    podmanExecutable,
    state,
    stopTimeoutSeconds: 10,
    supervisorId: SUPERVISOR_ID,
    writerCommand: Object.freeze(["/usr/local/bin/writer"]),
    writerEnvironment: exact({ LANG: "C.UTF-8" }),
  }));
  const input = launchInput(attachment, imageDigest);
  const receipt = await supervisor.launchWriter(input);
  assert.equal(process.pid, servicePid);
  assert.equal(process.getuid(), serviceUid);
  const markerPath = `${attachment.rootPath}/podman-writer-ready`;
  assert.equal(typeof receipt.stopWriter, "function");
  try {
    await waitForExt4PodmanReadyMarker(markerPath);
    const marker = await lstat(markerPath);
    assert.equal(marker.isFile(), true);
    assert.equal(marker.uid, serviceUid);
    assert.equal(marker.gid, process.getgid());
    assert.equal(marker.mode & 0o7777, 0o600);
  } finally {
    assert.deepEqual(await receipt.stopWriter(stopInput(input, receipt)), exact({
      contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
      status: "stopped",
    }));
  }
  assert.equal(process.pid, servicePid);
  assert.equal(process.getuid(), serviceUid);
  return exact({ markerPath, servicePid, serviceUid });
}

Object.freeze(runExt4PodmanWriterIntegration);
Object.freeze(retireExt4PodmanRootlessNamespaceForConformance);
Object.freeze(waitForExt4PodmanReadyMarker);
