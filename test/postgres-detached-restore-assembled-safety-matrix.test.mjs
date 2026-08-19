import assert from "node:assert/strict";
import test from "node:test";

import {
  createPostgresDetachedRestoreOperationalLeaseBudget,
  isPostgresDetachedRestoreOperationalLeaseBudget,
} from "../src/postgres-detached-restore-operational-lease-budget.mjs";
import {
  POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX,
  createAssembledRestorePublicationCallback,
} from "./fixtures/postgres-assembled-restore-safety-matrix/contract.mjs";

const EXPECTED_LEAF_KEYS = Object.freeze([
  "image.inspectCodex",
  "image.resolveImagePlan",
  "lifecycle.captureCheckpoint",
  "lifecycle.destroySession",
  "lifecycle.detachAttachment",
  "lifecycle.forceFence",
  "lifecycle.prepareRestoreAttachment",
  "lifecycle.prepareWritableAttachment",
  "lifecycle.provisionSession",
  "lifecycle.reconcileRestoreAttachment",
  "lifecycle.restoreCheckpoint",
  "publication.publishFreshCheckpointArtifact",
  "publication.publishRestoreDestination",
  "publication.verifyCommittedCheckpointArtifact",
  "publication.verifyCommittedRestoreDestination",
  "resolver.resolveRestoreDestination",
  "supervisor.launchWriter",
  "supervisor.reconcileWriterLaunch",
  "supervisor.stopWriter",
  "supervisorStateCollector.collectTerminalState",
]);
const EXPECTED_CUTS = Object.freeze({
  "checkpoint-capture": Object.freeze([
    "publication.publishFreshCheckpointArtifact",
    "plan.captureOperationId",
  ]),
  "restore-activation": Object.freeze([
    "lifecycle.prepareRestoreAttachment",
    "plan.activationOperationId",
  ]),
  "restore-generation": Object.freeze([
    "publication.publishRestoreDestination",
    "plan.request.operationId",
  ]),
  "supervisor-state-gc": Object.freeze([
    "supervisorStateCollector.collectTerminalState",
    "authorization.terminalOperationId",
  ]),
  "writer-force-fence": Object.freeze([
    "lifecycle.forceFence",
    "plan.detachOperationId",
  ]),
  "writer-launch": Object.freeze([
    "supervisor.launchWriter",
    "plan.launchAttemptId",
  ]),
  "writer-release": Object.freeze([
    "lifecycle.detachAttachment",
    "plan.detachOperationId",
  ]),
  "writer-stop": Object.freeze([
    "supervisor.stopWriter",
    "stopOperationId",
  ]),
});
const EXPECTED_OVERLAYS = Object.freeze({
  "fresh-publication": Object.freeze([
    "publication.publishFreshCheckpointArtifact",
    "publication.publishRestoreDestination",
  ]),
  "image-observation": Object.freeze([
    "image.inspectCodex",
    "image.resolveImagePlan",
  ]),
  "repeatable-observation": Object.freeze([
    "lifecycle.reconcileRestoreAttachment",
    "publication.verifyCommittedCheckpointArtifact",
    "publication.verifyCommittedRestoreDestination",
    "resolver.resolveRestoreDestination",
    "supervisor.reconcileWriterLaunch",
  ]),
  "storage-mutator": Object.freeze([
    "lifecycle.detachAttachment",
    "lifecycle.forceFence",
    "lifecycle.prepareRestoreAttachment",
  ]),
  "supervisor-mutator": Object.freeze([
    "supervisor.launchWriter",
    "supervisor.stopWriter",
  ]),
  "supervisor-state-mutator": Object.freeze([
    "supervisorStateCollector.collectTerminalState",
  ]),
});
const POLICY = Object.freeze({
  deadlineMilliseconds: 1,
  settlementGraceMilliseconds: 1,
});

function ownKeys(value) {
  return Reflect.ownKeys(value).sort();
}

function sorted(values) {
  return [...values].sort();
}

function assertExactFrozenRecord(value, keys) {
  assert.equal(Object.getPrototypeOf(value), null);
  assert.equal(Object.isFrozen(value), true);
  assert.deepEqual(ownKeys(value), sorted(keys));
}

function leavesByClassification(classification) {
  return POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX.leaves.filter(
    (leaf) => leaf.classification === classification,
  );
}

function policyMethods(policyGroup) {
  return POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX.leaves
    .filter((leaf) => leaf.policyGroup === policyGroup)
    .map((leaf) => leaf.method);
}

function policyRegistry(policyGroup) {
  return Object.fromEntries(
    policyMethods(policyGroup).map((method) => [method, POLICY]),
  );
}

function budgetOptions() {
  return {
    databaseRequestMilliseconds: 1,
    imagePlanProviderSettlement: policyRegistry(
      "imagePlanProviderSettlement",
    ),
    leaseDurationMilliseconds: 100,
    lifecycleBackendSettlement: policyRegistry(
      "lifecycleBackendSettlement",
    ),
    publicationSettlement: policyRegistry("publicationSettlement"),
    resolveRestoreDestinationSettlement: POLICY,
    safetyMarginMilliseconds: 1,
    supervisorSettlement: policyRegistry("supervisorSettlement"),
  };
}

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function publicationContext(publicationMode) {
  const request = exact({ operationId: "restore-generation-operation-001" });
  const result = exact({ request });
  return exact({
    artifactDirectory: "/artifact-root/artifact-001",
    artifactOwnedRoot: "/artifact-root",
    artifactProof: exact({ modeledDigest: "a".repeat(64) }),
    canonicalLease: exact({ leaseId: "lease-001" }),
    destinationDirectory: "/destination-root/restored-001",
    destinationIsolationProofId: "isolation-proof-001",
    destinationOwnedRoot: "/destination-root",
    destinationState: "detached",
    generationBinding: exact({ request }),
    now: "2026-08-12T00:00:00.000Z",
    publicationMode,
    reservationId: "reservation-001",
    result,
    storageRef: exact({ backendId: "storage-backend-001" }),
  });
}

test("assembled restore safety matrix fixes twenty exact settlement leaves", () => {
  const contract = POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX;
  assertExactFrozenRecord(contract, [
    "contractVersion",
    "cutKeys",
    "leaves",
    "overlayFamilies",
  ]);
  assert.equal(contract.contractVersion, 1);
  assert.equal(Object.isFrozen(contract.leaves), true);
  assert.equal(contract.leaves.length, 20);

  const counts = { "contract-only": 0, mutator: 0, observation: 0 };
  const leafKeys = new Set();
  for (const leaf of contract.leaves) {
    assertExactFrozenRecord(leaf, [
      "classification",
      "cutKey",
      "key",
      "method",
      "policyGroup",
    ]);
    assert.equal(Object.hasOwn(counts, leaf.classification), true);
    counts[leaf.classification] += 1;
    assert.equal(leafKeys.has(leaf.key), false, leaf.key);
    leafKeys.add(leaf.key);
    assert.equal(
      leaf.classification === "mutator",
      leaf.cutKey !== null,
      leaf.key,
    );
  }

  assert.deepEqual(counts, {
    "contract-only": 5,
    mutator: 8,
    observation: 7,
  });
  assert.deepEqual(sorted(leafKeys), EXPECTED_LEAF_KEYS);
});

test("eight durable cut keys map one-to-one to effectful mutators", () => {
  const { cutKeys } = POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX;
  const mutators = leavesByClassification("mutator");
  assert.equal(Object.isFrozen(cutKeys), true);
  assert.equal(cutKeys.length, 8);

  const observedCutKeys = new Set();
  const observedLeafKeys = new Set();
  for (const cut of cutKeys) {
    assertExactFrozenRecord(cut, ["durableKey", "key", "leafKey"]);
    assert.equal(observedCutKeys.has(cut.key), false, cut.key);
    assert.equal(observedLeafKeys.has(cut.leafKey), false, cut.leafKey);
    observedCutKeys.add(cut.key);
    observedLeafKeys.add(cut.leafKey);
    assert.deepEqual(EXPECTED_CUTS[cut.key], [cut.leafKey, cut.durableKey]);
    const leaf = mutators.find((candidate) => candidate.key === cut.leafKey);
    assert.notEqual(leaf, undefined, cut.leafKey);
    assert.equal(leaf.cutKey, cut.key);
  }

  assert.deepEqual(
    sorted(observedCutKeys),
    sorted(Reflect.ownKeys(EXPECTED_CUTS)),
  );
  assert.deepEqual(
    sorted(observedLeafKeys),
    sorted(mutators.map((leaf) => leaf.key)),
  );
});

test("six settlement overlays partition the declared protocol surface exactly once", () => {
  const { overlayFamilies } = POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX;
  const reachable = [
    ...leavesByClassification("mutator"),
    ...leavesByClassification("observation"),
  ];
  const contractOnly = new Set(
    leavesByClassification("contract-only").map((leaf) => leaf.key),
  );
  assert.equal(Object.isFrozen(overlayFamilies), true);
  assert.equal(overlayFamilies.length, 6);

  const overlayKeys = new Set();
  const coveredLeaves = new Set();
  for (const overlay of overlayFamilies) {
    assertExactFrozenRecord(overlay, ["key", "leafKeys"]);
    assert.equal(Object.isFrozen(overlay.leafKeys), true);
    assert.equal(overlayKeys.has(overlay.key), false, overlay.key);
    overlayKeys.add(overlay.key);
    assert.deepEqual(sorted(overlay.leafKeys), EXPECTED_OVERLAYS[overlay.key]);
    for (const leafKey of overlay.leafKeys) {
      assert.equal(contractOnly.has(leafKey), false, leafKey);
      assert.equal(coveredLeaves.has(leafKey), false, leafKey);
      coveredLeaves.add(leafKey);
    }
  }

  assert.deepEqual(
    sorted(overlayKeys),
    sorted(Reflect.ownKeys(EXPECTED_OVERLAYS)),
  );
  assert.deepEqual(
    sorted(coveredLeaves),
    sorted(reachable.map((leaf) => leaf.key)),
  );
});

test("the explicit publication seam routes fresh mutation and committed observation separately", async () => {
  const calls = [];
  const publication = exact({
    async publishRestoreDestination(options) {
      calls.push({ method: "publishRestoreDestination", options });
      return exact({ method: "fresh" });
    },
    async verifyCommittedRestoreDestination(options) {
      calls.push({ method: "verifyCommittedRestoreDestination", options });
      return exact({ method: "committed" });
    },
  });
  const publish = createAssembledRestorePublicationCallback(publication);
  const freshContext = publicationContext("fresh-or-exact-replay");
  const committedContext = publicationContext("committed-only");

  assert.deepEqual(await publish(freshContext), exact({ method: "fresh" }));
  assert.deepEqual(
    await publish(committedContext),
    exact({ method: "committed" }),
  );
  assert.deepEqual(
    calls.map(({ method }) => method),
    ["publishRestoreDestination", "verifyCommittedRestoreDestination"],
  );
  assert.deepEqual(ownKeys(calls[0].options), [
    "artifactDirectory",
    "artifactOwnedRoot",
    "artifactProof",
    "binding",
    "destinationDirectory",
    "destinationOwnedRoot",
    "operationId",
    "request",
    "result",
  ]);
  assert.deepEqual(ownKeys(calls[1].options), [
    "artifactProof",
    "binding",
    "destinationDirectory",
    "destinationOwnedRoot",
    "operationId",
    "request",
    "result",
  ]);
  for (let index = 0; index < calls.length; index += 1) {
    const context = index === 0 ? freshContext : committedContext;
    assert.equal(Object.isFrozen(calls[index].options), true);
    assert.equal(Object.getPrototypeOf(calls[index].options), null);
    assert.strictEqual(calls[index].options.binding, context.generationBinding);
    assert.strictEqual(calls[index].options.request, context.generationBinding.request);
    assert.strictEqual(calls[index].options.result, context.result);
    assert.equal(
      calls[index].options.operationId,
      context.generationBinding.request.operationId,
    );
  }

  await assert.rejects(
    createAssembledRestorePublicationCallback(publication)(
      publicationContext("publish-again"),
    ),
    TypeError,
  );
  assert.equal(calls.length, 2);
});

test("declared leaves match the production policy surfaces", () => {
  const budget = createPostgresDetachedRestoreOperationalLeaseBudget(
    budgetOptions(),
  );
  assert.equal(isPostgresDetachedRestoreOperationalLeaseBudget(budget), true);
  assert.deepEqual(
    {
      image: sorted(policyMethods("imagePlanProviderSettlement")),
      lifecycle: sorted(policyMethods("lifecycleBackendSettlement")),
      publication: sorted(policyMethods("publicationSettlement")),
      resolver: sorted(policyMethods("resolveRestoreDestinationSettlement")),
      supervisor: sorted(policyMethods("supervisorSettlement")),
      supervisorStateCollector: sorted(
        policyMethods("supervisorStateCollectionSettlement"),
      ),
    },
    {
      image: ["inspectCodex", "resolveImagePlan"],
      lifecycle: [
        "captureCheckpoint",
        "destroySession",
        "detachAttachment",
        "forceFence",
        "prepareRestoreAttachment",
        "prepareWritableAttachment",
        "provisionSession",
        "reconcileRestoreAttachment",
        "restoreCheckpoint",
      ],
      publication: [
        "publishFreshCheckpointArtifact",
        "publishRestoreDestination",
        "verifyCommittedCheckpointArtifact",
        "verifyCommittedRestoreDestination",
      ],
      resolver: ["resolveRestoreDestination"],
      supervisor: [
        "launchWriter",
        "reconcileWriterLaunch",
        "stopWriter",
      ],
      supervisorStateCollector: ["collectTerminalState"],
    },
  );
});

test("contract-only lifecycle leaves remain configuration-visible but outside durable cuts", () => {
  const contractOnly = leavesByClassification("contract-only");
  const cutLeafKeys = new Set(
    POSTGRES_ASSEMBLED_RESTORE_SAFETY_MATRIX.cutKeys.map(
      (cut) => cut.leafKey,
    ),
  );
  assert.deepEqual(
    sorted(contractOnly.map((leaf) => leaf.method)),
    [
      "captureCheckpoint",
      "destroySession",
      "prepareWritableAttachment",
      "provisionSession",
      "restoreCheckpoint",
    ],
  );

  for (const leaf of contractOnly) {
    assert.equal(cutLeafKeys.has(leaf.key), false, leaf.key);
    const options = budgetOptions();
    delete options.lifecycleBackendSettlement[leaf.method];
    assert.throws(
      () => createPostgresDetachedRestoreOperationalLeaseBudget(options),
      {
        code:
          "invalid_postgres_detached_restore_operational_lease_budget_options",
      },
    );
  }
});
