---
id: 20260812-6d3a91
title: Assembled Restore Safety Matrix
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/assembled-restore-safety-matrix
pr:
supersedes: []
superseded_by:
---

# Assembled Restore Safety Matrix

## Summary

- The assembled detached-restore contract and evidence matrix is complete; the
  final public backend remains intentionally separate.
- Preserve the distinction between an at-most-once durable mutation and a
  repeatable trusted read-only observation.
- Keep the production checkpoint adapter's fixed fail-closed `runRestore()`
  stub unchanged in this slice.

## Current State

- Deployment owns nineteen method-specific physical settlement contracts.
  Fourteen belong to the private assembled protocol surface; five remain
  generic lifecycle contracts outside that protocol.
- The test-only matrix contract freezes that classification, its seven durable
  cut keys, and the `supervisor-mutator`, `storage-mutator`,
  `fresh-publication`, `repeatable-observation`, and `image-observation`
  settlement overlays, and checks the declared leaves against the production
  policy surface. The deployment fake-PostgreSQL overlay covers image
  deadline-late settlement, independent image observation retry, stop
  abort/drain, grace-breach fatal shutdown, and zero calls to the four durable
  collaborator categories it cannot reach. It is not five-family execution.
- A test-only callback router exercises both choices at the explicit foreground
  publication seam: `fresh-or-exact-replay` invokes
  `publishRestoreDestination()`, while `committed-only` invokes
  `verifyCommittedRestoreDestination()` without source-path inputs. This binds
  the protocol edge but is not the final public adapter.
- The safety harness must call
  `deployment.foreground.runRestore(admission, publish)`. The `publish` callback
  is an explicit composition seam, not an automatically wired public backend.
  It receives the exact frozen null-prototype generation-publication context
  with `artifactDirectory`, `artifactOwnedRoot`, `artifactProof`,
  `canonicalLease`, `destinationDirectory`, `destinationIsolationProofId`,
  `destinationOwnedRoot`, `destinationState`, `generationBinding`, `now`,
  `publicationMode`, `reservationId`, `result`, and `storageRef`, then returns an exact frozen
  `{materialization, replayed, result}` completion. A
  `committed-only` context accepts only `replayed: true`.
- Seven existing real-PostgreSQL acknowledgement-loss/replay paths correspond
  one-to-one with the seven mutators. The aggregation checks their exact
  persisted operation identity, expected state, one reservation, and no repeat
  grant or finalization where applicable.
- Runtime-controller coverage now constructs fresh physical bindings, image
  binding, runtime, and controller over the same PostgreSQL state and stable
  plan after the old controller stops. The retry leaves the durable snapshot
  unchanged, does not grow old-runtime mutator counts, and enters only the new
  artifact/publication observation path. Stable-plan coverage separately
  constructs a new registry and rehydrates the same plan digest. This remains
  in-process object replacement, not one deployment A-to-B whole-saga restart.
  Terminal runtime/controller objects, opaque image reservations, and writer
  handles are not reused.
- The fake-PostgreSQL fixture cannot represent the durable authority graph.
  Durable cuts and acknowledgement loss therefore remain real-PostgreSQL
  evidence; neither layer claims an operating-system process kill.

## Reachability Classification

The seven grant-bearing mutators are:

- `supervisor.stopWriter`;
- `publication.publishFreshCheckpointArtifact`;
- `publication.publishRestoreDestination`;
- `lifecycle.detachAttachment`;
- `lifecycle.forceFence`;
- `lifecycle.prepareRestoreAttachment`; and
- `supervisor.launchWriter`.

The seven repeatable observations are:

- `supervisor.reconcileWriterLaunch`;
- `lifecycle.reconcileRestoreAttachment`;
- `publication.verifyCommittedCheckpointArtifact`;
- `publication.verifyCommittedRestoreDestination`;
- `resolver.resolveRestoreDestination`;
- `image.resolveImagePlan`; and
- `image.inspectCodex`.

The five contract-only lifecycle leaves are:

- `lifecycle.captureCheckpoint`;
- `lifecycle.destroySession`;
- `lifecycle.prepareWritableAttachment`;
- `lifecycle.provisionSession`; and
- `lifecycle.restoreCheckpoint`.

## Safety Contract

- A deadline, abort, late settlement, grace breach, stop, or reconstruction does
  not automatically reissue the same settlement invocation.
- Each durable mutator may dispatch at most once for its exact operation grant.
  Database commit-acknowledgement loss may permit exact durable readback or
  finalizer replay, never another physical mutation.
- A distinct recovery attempt may repeat a trusted resolver, verifier,
  inspector, or reconciler. Repeated image resolution and inspection may mint a
  new process-local opaque reservation for the same fixed prepared plan. These
  observations cannot create a grant, alter the fixed request, or prove writer
  authority.
- Graceful deployment `stop()` drains admitted work and settlements; it is not
  evidence of a process crash. This matrix records durable replay and separate
  new-object reconstruction evidence without treating graceful shutdown as an
  operating-system crash.

## Matrix Scope

- Contract layer: exact 19-leaf classification, 7 mutator-to-durable-cut keys,
  7 repeatable observations, 5 contract-only lifecycle leaves, and 5
  semantic overlay partitions checked against the production policy surface.
- Real PostgreSQL: aggregate the existing writer stop, checkpoint capture,
  restore generation, writer release, writer force-fence, restore activation,
  and writer launch acknowledgement-loss/replay evidence and re-read their
  persisted authority rows.
- Reconstruction: reference the existing separate new-runtime composition and
  new stable-plan-registry readback cases over the same PostgreSQL state. Do not
  describe them as one deployment whole-saga restart.
- Physical settlement: retain the foundation's aggregate stop ownership and
  representative timer/grace tests, plus deployment image deadline-late retry,
  abort/drain, grace-breach fatal shutdown, and zero-call evidence. Do not infer
  fake-PostgreSQL execution of supervisor, lifecycle, publication, or resolver
  durable work.
- Exclusions: no operating-system `SIGKILL`, no production restart API, no
  final public backend, and no filesystem/ext4 physical execution.

## Next Steps

- Construct the final public restore-capable backend and enable the production
  adapter only if it preserves the completed matrix's no-second-writer
  boundary.
- After that adapter, implement filesystem/ext4 physical execution and its
  cross-host conformance evidence.

## Validation

- The contract and operational-lease focused tests passed 12/12.
- The deployment fake-PostgreSQL focused scenarios passed 32/32.
- The matrix/foreground integration tests passed 80/80, and the wider affected
  restore suite completed successfully.
- The repository-wide suite completed successfully with external installed-
  Codex probes deliberately unavailable. With the ambient installed Codex
  enabled, its auth probe instead reproduced the host-level
  `EMFILE: too many open files, watch` failure; no matrix assertion failed.
- `node --check integration/postgres-session-authority.mjs` passed for the
  real-PostgreSQL additions. `npm run test:postgres` stopped at its required
  `SESSION_AUTHORITY_DATABASE_URL` preflight because that variable is not
  configured locally; the gate remains CI/environment-dependent and is not
  represented as an operating-system crash test.
- Project-journal validation and `git diff --check` passed after documentation
  closure.

## Evidence

- `test/fixtures/postgres-assembled-restore-safety-matrix/contract.mjs`
- `test/postgres-detached-restore-assembled-safety-matrix.test.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
- `test/fixtures/postgres-deployment/scenario.mjs`
- `integration/postgres-session-authority.mjs`
- `test/postgres-detached-restore-foreground-composition.test.mjs`
- `test/postgres-detached-restore-physical-bindings.test.mjs`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
