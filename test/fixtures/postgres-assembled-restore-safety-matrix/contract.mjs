function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function frozen(values) {
  return Object.freeze([...values]);
}

function leaf(classification, cutKey, key, method, policyGroup) {
  return exact({ classification, cutKey, key, method, policyGroup });
}

function cut(durableKey, key, leafKey) {
  return exact({ durableKey, key, leafKey });
}

function overlay(key, leafKeys) {
  return exact({ key, leafKeys: frozen(leafKeys) });
}

const PUBLICATION_CONTEXT_KEYS = frozen([
  "artifactDirectory",
  "artifactOwnedRoot",
  "artifactProof",
  "canonicalLease",
  "destinationDirectory",
  "destinationIsolationProofId",
  "destinationOwnedRoot",
  "destinationState",
  "generationBinding",
  "now",
  "publicationMode",
  "reservationId",
  "result",
  "storageRef",
]);

function exactContext(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    Object.getPrototypeOf(value) !== null ||
    !Object.isFrozen(value) ||
    Reflect.ownKeys(value).length !== PUBLICATION_CONTEXT_KEYS.length
  ) {
    throw new TypeError("assembled restore publication context is invalid");
  }
  for (const key of PUBLICATION_CONTEXT_KEYS) {
    if (!Object.hasOwn(value, key)) {
      throw new TypeError("assembled restore publication context is invalid");
    }
  }
  return value;
}

function exactPublicationFunction(publication, method) {
  const descriptor = Object.getOwnPropertyDescriptor(publication, method);
  if (
    descriptor?.enumerable !== true ||
    typeof descriptor.value !== "function"
  ) {
    throw new TypeError("assembled restore publication collaborator is invalid");
  }
  return descriptor.value;
}

export function createAssembledRestorePublicationCallback(publication) {
  if (
    publication === null ||
    typeof publication !== "object" ||
    Object.getPrototypeOf(publication) !== null ||
    !Object.isFrozen(publication)
  ) {
    throw new TypeError("assembled restore publication collaborator is invalid");
  }
  const publishRestoreDestination = exactPublicationFunction(
    publication,
    "publishRestoreDestination",
  );
  const verifyCommittedRestoreDestination = exactPublicationFunction(
    publication,
    "verifyCommittedRestoreDestination",
  );
  return Object.freeze(async function assembledRestorePublication(context) {
    if (arguments.length !== 1) {
      throw new TypeError("assembled restore publication context is invalid");
    }
    const normalized = exactContext(context);
    const binding = normalized.generationBinding;
    if (
      binding === null ||
      typeof binding !== "object" ||
      binding.request === null ||
      typeof binding.request !== "object" ||
      typeof binding.request.operationId !== "string"
    ) {
      throw new TypeError("assembled restore publication context is invalid");
    }
    if (normalized.publicationMode === "fresh-or-exact-replay") {
      return Reflect.apply(publishRestoreDestination, publication, [
        exact({
          artifactDirectory: normalized.artifactDirectory,
          artifactOwnedRoot: normalized.artifactOwnedRoot,
          artifactProof: normalized.artifactProof,
          binding,
          destinationDirectory: normalized.destinationDirectory,
          destinationOwnedRoot: normalized.destinationOwnedRoot,
          operationId: binding.request.operationId,
          request: binding.request,
          result: normalized.result,
        }),
      ]);
    }
    if (normalized.publicationMode === "committed-only") {
      return Reflect.apply(verifyCommittedRestoreDestination, publication, [
        exact({
          artifactProof: normalized.artifactProof,
          binding,
          destinationDirectory: normalized.destinationDirectory,
          destinationOwnedRoot: normalized.destinationOwnedRoot,
          operationId: binding.request.operationId,
          request: binding.request,
          result: normalized.result,
        }),
      ]);
    }
    throw new TypeError("assembled restore publication mode is invalid");
  });
}

const leaves = frozen([
  leaf(
    "mutator",
    "supervisor-state-gc",
    "supervisorStateCollector.collectTerminalState",
    "collectTerminalState",
    "supervisorStateCollectionSettlement",
  ),
  leaf(
    "mutator",
    "writer-stop",
    "supervisor.stopWriter",
    "stopWriter",
    "supervisorSettlement",
  ),
  leaf(
    "mutator",
    "checkpoint-capture",
    "publication.publishFreshCheckpointArtifact",
    "publishFreshCheckpointArtifact",
    "publicationSettlement",
  ),
  leaf(
    "mutator",
    "restore-generation",
    "publication.publishRestoreDestination",
    "publishRestoreDestination",
    "publicationSettlement",
  ),
  leaf(
    "mutator",
    "writer-release",
    "lifecycle.detachAttachment",
    "detachAttachment",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "mutator",
    "writer-force-fence",
    "lifecycle.forceFence",
    "forceFence",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "mutator",
    "restore-activation",
    "lifecycle.prepareRestoreAttachment",
    "prepareRestoreAttachment",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "mutator",
    "writer-launch",
    "supervisor.launchWriter",
    "launchWriter",
    "supervisorSettlement",
  ),
  leaf(
    "observation",
    null,
    "supervisor.reconcileWriterLaunch",
    "reconcileWriterLaunch",
    "supervisorSettlement",
  ),
  leaf(
    "observation",
    null,
    "lifecycle.reconcileRestoreAttachment",
    "reconcileRestoreAttachment",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "observation",
    null,
    "publication.verifyCommittedCheckpointArtifact",
    "verifyCommittedCheckpointArtifact",
    "publicationSettlement",
  ),
  leaf(
    "observation",
    null,
    "publication.verifyCommittedRestoreDestination",
    "verifyCommittedRestoreDestination",
    "publicationSettlement",
  ),
  leaf(
    "observation",
    null,
    "resolver.resolveRestoreDestination",
    "resolveRestoreDestination",
    "resolveRestoreDestinationSettlement",
  ),
  leaf(
    "observation",
    null,
    "image.resolveImagePlan",
    "resolveImagePlan",
    "imagePlanProviderSettlement",
  ),
  leaf(
    "observation",
    null,
    "image.inspectCodex",
    "inspectCodex",
    "imagePlanProviderSettlement",
  ),
  leaf(
    "contract-only",
    null,
    "lifecycle.captureCheckpoint",
    "captureCheckpoint",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "contract-only",
    null,
    "lifecycle.destroySession",
    "destroySession",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "contract-only",
    null,
    "lifecycle.prepareWritableAttachment",
    "prepareWritableAttachment",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "contract-only",
    null,
    "lifecycle.provisionSession",
    "provisionSession",
    "lifecycleBackendSettlement",
  ),
  leaf(
    "contract-only",
    null,
    "lifecycle.restoreCheckpoint",
    "restoreCheckpoint",
    "lifecycleBackendSettlement",
  ),
]);

const cutKeys = frozen([
  cut(
    "authorization.terminalOperationId",
    "supervisor-state-gc",
    "supervisorStateCollector.collectTerminalState",
  ),
  cut("stopOperationId", "writer-stop", "supervisor.stopWriter"),
  cut(
    "plan.captureOperationId",
    "checkpoint-capture",
    "publication.publishFreshCheckpointArtifact",
  ),
  cut(
    "plan.request.operationId",
    "restore-generation",
    "publication.publishRestoreDestination",
  ),
  cut(
    "plan.detachOperationId",
    "writer-release",
    "lifecycle.detachAttachment",
  ),
  cut(
    "plan.detachOperationId",
    "writer-force-fence",
    "lifecycle.forceFence",
  ),
  cut(
    "plan.activationOperationId",
    "restore-activation",
    "lifecycle.prepareRestoreAttachment",
  ),
  cut(
    "plan.launchAttemptId",
    "writer-launch",
    "supervisor.launchWriter",
  ),
]);

const overlayFamilies = frozen([
  overlay("supervisor-state-mutator", [
    "supervisorStateCollector.collectTerminalState",
  ]),
  overlay("supervisor-mutator", [
    "supervisor.stopWriter",
    "supervisor.launchWriter",
  ]),
  overlay("storage-mutator", [
    "lifecycle.detachAttachment",
    "lifecycle.forceFence",
    "lifecycle.prepareRestoreAttachment",
  ]),
  overlay("fresh-publication", [
    "publication.publishFreshCheckpointArtifact",
    "publication.publishRestoreDestination",
  ]),
  overlay("repeatable-observation", [
    "supervisor.reconcileWriterLaunch",
    "lifecycle.reconcileRestoreAttachment",
    "publication.verifyCommittedCheckpointArtifact",
    "publication.verifyCommittedRestoreDestination",
    "resolver.resolveRestoreDestination",
  ]),
  overlay("image-observation", [
    "image.resolveImagePlan",
    "image.inspectCodex",
  ]),
]);

export const POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX = exact({
  contractVersion: 1,
  cutKeys,
  leaves,
  overlayFamilies,
});
