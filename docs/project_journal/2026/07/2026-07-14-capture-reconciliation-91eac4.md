---
id: 20260714-91eac4
title: Committed Capture Reconciliation
status: completed
created: 2026-07-14
updated: 2026-07-14
branch: wip/capture-reconciliation
pr: 12
supersedes: []
superseded_by:
---

# Committed Capture Reconciliation

## Summary

- Added authenticated durable capture-attempt provenance to every new normal
  stopped-directory capture.
- Added a source-free, committed-only reconciliation path across the optional
  storage extension, snapshot core, mutation authority, backend, and physical
  publication verifier.

## Current State

- Normal capture still consumes exactly one same-process stopped-writer
  capability and atomically requires an absent journal operation. Inside that
  one-shot callback the backend generates a fresh UUID; before publication,
  the trusted mutation authority atomically claims it in a globally unique
  durable ledger and creates the canonical attempt for the v2 coordinator
  binding.
- Attempt-ID and operation claims are never reusable. Durable tombstones must
  outlive every journal, artefact, snapshot, backup, and DR generation that can
  reintroduce an old value, and the authority ledger must not roll back with a
  session data volume.
- Both active claim indexes bind the same canonical attempt record. Retirement
  atomically transitions them to one non-authorizing tombstone, so reinserting
  a structural copy of the old attempt cannot revive reconciliation authority.
- Normal capture, reconciliation, and retirement share per-operation
  serialization. Reconciliation revalidates after admission waits and
  immediately before verification. Durable finalization revalidates the
  expected attempt plus both active claim indexes after asynchronous
  verification so a concurrent tombstone cannot be overwritten.
- `reconcileCleanCheckpointCapture()` accepts the original descriptor and
  checkpoint request without a current lease, clock, attachment, writer, or
  stopped-writer capability. It dispatches only to a backend advertising the
  separately versioned reconciliation extension.
- The stopped-directory backend asks the trusted authority to load the exact
  durable attempt, then byte-matches its binding, request, and predetermined
  result against the committed physical journal record.
- `verifyCommittedCheckpointArtifact()` accepts no source path and performs no
  journal transition or namespace mutation. It validates target and journal
  identities, committed topology and materialisation, the exact checkpoint
  bundle, manifest, payload, modeled digest, and tree identity before replaying
  the recorded result.
- `absent`, `prepared`, and `materialized` states never become success through
  this API. They remain retained evidence for a separately authorized operator
  repair design.
- Legacy v1 capture bindings have no authenticated attempt provenance and are
  not automatically upgraded or reconciled.
- Adapter v2 upgrades only the capture coordinator binding. Restore bindings
  remain at v1 so exact committed restores from adapter v1 still replay after
  an upgrade.
- The conformance mutation authority proves the seam and failure model. A
  production linearizable database, catalogue, fence service, and launcher
  admission implementation remain deferred.
- Hostile regressions prove that a pre-existing committed artefact cannot be
  authenticated by a fresh attempt and that a durable attempt-ID collision is
  rejected before the publication callback, including after the canonical
  attempt record itself has been retired.
- Git Summary remains deferred user context and is not reconciliation
  authority.

## Next Steps

- Verify same-image Codex resume from a committed restored checkpoint and add
  rollout-tail repair where the pinned runtime evidence requires it.
- Implement the production linearizable authority and an ext4 or
  filesystem-image physical backend before differential export and cross-host
  migration verification.

## Evidence

- `docs/architecture/session-storage-contracts.md`
- `docs/architecture/snapshot-restore-core.md`
- `docs/architecture/stopped-directory-publication.md`
- `docs/architecture/stopped-directory-backend.md`
- `src/session-storage-contracts.mjs`
- `src/session-snapshot-core.mjs`
- `src/stopped-directory-publication.mjs`
- `src/stopped-directory-backend.mjs`
- `test/session-storage-contracts.test.mjs`
- `test/session-snapshot-core.test.mjs`
- `test/stopped-directory-publication.test.mjs`
- `test/stopped-directory-backend.test.mjs`
- PR #12
