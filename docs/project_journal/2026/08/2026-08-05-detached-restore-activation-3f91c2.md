---
id: 20260805-3f91c2
title: Detached Restore Activation and Recovery Composition
status: completed
created: 2026-08-05
updated: 2026-08-06
branch: wip/detached-restore-activation
pr: 30
supersedes: []
superseded_by:
---

# Detached Restore Activation and Recovery Composition

## Summary

This slice implements the authority boundary between an independently published
restore destination and the next prepared writer launch. The slice keeps
production `runRestore()` fail-closed while adding source-free committed
verification, provider-backed object activation, one atomic canonical
attachment-to-launch transition, and bounded recovery that never relaunches or
reconstructs an opaque process-local capability.

## Protected Property

The prepared launch must bind the exact filesystem object durably published by
the authorized restore generation and the exact current provider-backed
attachment authority for that object. The old writer and attachment must
already be stopped, physically fenced, and canonically detached before the new
attachment can become authoritative.

Object identity, content stability, and access policy are separate proofs:

- persisted filesystem and object identity bind the final directory to the
  committed publication;
- the committed materialization digest binds the restored tree contents;
- the provider activation proof binds that same object to the new attachment,
  lease, fence, manifest, storage reference, and backend authority;
- canonical path equality is retained only as correlation evidence and never
  substitutes for any of those proofs.

Benign metadata churn is not object replacement. Revalidation rejects an
object-ID, filesystem-ID, tree-identity, or provider-authority mismatch and
reports unreadable or failed revalidation separately from absence.

## Current State

> [!NOTE]
> Later production-composition exploration proved that activation request
> version 1's direct stop-to-detach and same-generation predecessor cannot
> consume the durable stop-to-clean-capture result. Version 1 remains valid for
> its exact historical replay contract. The capture-bound request version 2
> correction is tracked in
> `docs/project_journal/2026/08/2026-08-06-capture-bound-restore-activation-c4a2d8.md`.

- `verifyCommittedRestoreDestination()` is source-free and read-only. It
  accepts only the exact committed restore journal/publication identity,
  revalidates the detached destination's object, content, and access-policy
  bindings, and never advances absent, prepared, or materialized state.
- The optional version 1 restore-attachment provider contract binds the
  committed publication object and materialization digest to the exact attach
  mutation, writer fence, and provider proof echoes. Canonical path equality is
  correlation only.
- Typed `restore-attachment-activation-v1` authority preclaims the exact launch
  identity, dispatches provider activation outside the transaction, and
  serializably finalizes the new canonical attachment plus predetermined
  prepared launch as one atomic transition. Exact readback resolves commit
  acknowledgement loss without a second provider activation.
- The recovery coordinator consumes only durable candidates and committed
  publication evidence. It can reconcile retained generation, activation, and
  launch-attempt work without relaunching or reconstructing an opaque image,
  writer, or stopped-writer capability.
- The bounded recovery service supplies four independent keyset lanes:
  generation, attachment activation, prepared or active launch attempt, and
  current-launch inventory. It admits one sequential batch at a time, drains
  admitted native-Promise work before advancing a cursor, and treats current
  launches as inventory requiring an explicit stop or fence.
- Production `runRestore()` remains fail-closed. Production adapter enablement
  is a separate pending slice, and Git Summary remains deferred.

## Design Boundary

1. The read-only `verifyCommittedRestoreDestination()` path accepts only
   the immutable restore journal/publication identity and detached destination
   authority. It must not inspect the capture source or advance an absent,
   prepared, or materialized operation.
2. The narrowly versioned restore-attachment activation provider extension
   keeps the generic storage contract and ordinary attachment result unchanged.
   The provider result binds its attachment proof to the exact committed
   publication object identity and materialization digest.
3. The new versioned restore activation request/binding preserves existing
   v1/v2 restore-generation semantics. It predeclares deterministic new lease,
   attachment, provider mutation, and launch-attempt identities, and authorizes
   activation only from a canonical `DETACHED` session that retains evidence
   that the old attachment was stopped and fenced.
4. One serializable finalizer validates the committed generation,
   source-free publication verification, provider attachment result, and
   predeclared identities; atomically installs the new canonical attachment and
   materializes the exact prepared launch operation. A failed transaction must
   expose neither activation nor launch work.
5. The bounded recovery service uses independent cursors for generation,
   attachment activation, prepared/active launch attempts, and current-launch
   inventory.
   It may verify, attach idempotently, finalize, cancel an expired prepared
   attempt, or reconcile stopped-only evidence. It must never publish, copy,
   rename, reserve or consume an image, call the launch callback, adopt an
   opaque writer handle, or synthesize a capability.

## Dependency Order

1. Source-free committed restore verification and exact publication binding.
2. Versioned provider activation contract and authority request shape.
3. Atomic generation/attachment/prepared-launch transition.
4. Four-lane bounded recovery composition.
5. Documentation plus focused publication, coordinator, and service tests.

## Acceptance Matrix

- Publication verification classifies absent, prepared, candidate-only,
  final-only, committed, unreadable, replaced-object, changed-content, and
  changed-access-policy states without creating or advancing journal state.
- A real existing old attachment and a distinct absent restore destination are
  exercised end to end.
- Correct path with wrong object identity, same-path object replacement, and
  different-path views of the same object cannot bypass provider binding.
- No activation occurs while stop, fence, detach, publication, or provider
  evidence is uncertain.
- Crashes after publication commit, after provider success, and after atomic
  database commit replay the same generation, provider operation/proof,
  attachment, and launch attempt exactly once.
- Activation plus prepared-launch reservation is all-or-nothing, including
  serializable conflict and lost commit acknowledgement cases.
- Prepared recovery never reserves an image or launches. Starting/uncertain
  recovery uses only retained provisional evidence or stopped-only supervisor
  reconciliation. Current-launch discovery is inventory only.
- Existing v1/v2 restore, writer acquisition, generation, launch-attempt,
  storage-provider, and publication replay contracts remain compatible.

## Non-Goals

- No production `runRestore()` enablement.
- No second physical publication, capture-source read, or path-only
  authorization during recovery.
- No opaque image reservation, writer handle, or stopped-writer capability
  reconstruction after restart.
- No concrete Podman, Docker, filesystem-image, differential-export, or
  cross-host backend.
- No Git Summary implementation.

## Next Steps

1. Capture-bound activation request version 2 is implemented in the successor
   workstream. Keep production `runRestore()` disabled until the separate
   production adapter workstream passes its invocation-time fleet capability
   gate and composes the full protocol under ambiguous-outcome coverage.
2. Leave filesystem-image export, differential compression, cross-host restore,
   and Git Summary to their existing deferred workstreams.

## Evidence

- `docs/project_journal/2026/08/2026-08-05-restore-publication-attachment-contract-4c7a91.md`
- `docs/project_journal/2026/08/2026-08-05-durable-stop-capture-composition-7e3a91.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
- `docs/architecture/stopped-directory-publication.md`
- Focused committed-destination publication coverage: 13/13 passing; full
  `test/stopped-directory-publication.test.mjs`: 178/178 passing.
- `test/postgres-restore-activation-recovery-coordinator.test.mjs`: 55/55
  passing; coverage includes canonical authority receipt reconstruction,
  source-free generation replay, activation acknowledgement loss, provider
  key-order normalization, and prepared/starting/uncertain/terminal launch
  handoffs.
- `test/postgres-session-operation-kernel.test.mjs`: 210/210 passing,
  including activation-created prepared, starting, and uncertain launch
  recovery provenance.
- The combined coordinator and operation-kernel run completed 265/265
  passing.
- `node --check src/postgres-restore-activation-recovery-service.mjs` and
  `test/postgres-restore-activation-recovery-service.test.mjs`: 13/13 passing.
- Focused read-only service review after thenable and storage-binding fixes:
  `No findings.`
- Coordinator/authority receipt-fidelity review after the generation-document
  v2 correction: `No findings.`
- Independent migration, authority, lock-order, revision-budget, and replay
  audit: `No findings.`
- Final independent full-diff and test-contract audits after the receipt,
  canonical-order, snapshot-history, and reachable-fixture fixes:
  `No findings.`
- Node.js `v24.18.0` from the installed runtime inventory:
  `npm test -- --test-reporter=dot` completed with exit 0. The managed sandbox
  made the unchanged live app-server `fs.watch` test fail with `EMFILE`; the
  exact test and the complete suite both passed outside that sandbox.
- The real PostgreSQL integration gate was not locally executable because
  `SESSION_AUTHORITY_DATABASE_URL` and a local PostgreSQL runtime were absent;
  `.github/workflows/test.yml` runs it against PostgreSQL 18.4 for the PR.
