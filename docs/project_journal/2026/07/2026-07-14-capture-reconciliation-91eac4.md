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
  capability and atomically requires an absent journal operation. Before
  publication, the trusted mutation authority durably creates the canonical
  attempt and supplies its opaque attempt ID for the v2 coordinator binding.
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
