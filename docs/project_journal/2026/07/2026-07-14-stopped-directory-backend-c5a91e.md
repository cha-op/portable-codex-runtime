---
id: 20260714-c5a91e
title: Stopped-Directory Backend
status: completed
created: 2026-07-14
updated: 2026-07-14
branch: wip/stopped-directory-backend
pr: 11
supersedes: []
superseded_by:
---

# Stopped-Directory Backend

## Summary

- Added the v1 stopped-directory storage backend that composes one-use
  stopped-writer authority, durable mutation admission, local publication, and
  the snapshot core's exact result contract.
- Kept lifecycle ownership in a separately validated same-backend adapter and
  declared the composed backend's limits as local directory attachments,
  exclusive writers, manual fencing, and no atomic point-in-time snapshot.

## Current State

- The adapter owns only `captureCheckpoint` and `restoreCheckpoint`.
  Provision, writable attachment preparation, detach, force-fence, and destroy
  operations delegate to a validated lifecycle backend with the same backend
  ID.
- Capture resolves one exact stopped writer synchronously and consumes the
  supplied process-local capability through the designated coordinator.
  Writer handles and capabilities never enter mutation admission, publication
  bindings, checkpoint descriptors, or durable records.
- The capture authority durably reserves the exact request, descriptor, and
  predetermined result while holding the canonical writer-fence and admission
  guard. Publication validates the current clock, fence, storage, attachment,
  path plan, and versioned coordinator binding before materializing an
  artefact.
- Restore requires a newer canonical fence, trusted committed artefact proof,
  an isolated detached destination, and the same guarded predetermined-result
  protocol before publishing a payload-only destination.
- Each authority operation must await exactly one callback and durably finalize
  the catalogue or destination before returning the exact callback completion.
  Zero, multiple, late, or substituted callback results fail closed. The
  backend observes every callback Promise rejection without replacing the
  Promise returned to the authority, so discarded or late calls cannot escape
  as process-level unhandled rejections.
- Deterministic request-shape failures occur before collaborator dispatch.
  Runtime collaborator failures are frozen, non-retryable, path-free backend
  uncertainty, and capture uncertainty terminally consumes the capability.
- Exact committed-result replay inside a currently authorized transaction is
  supported. Replay-only reconciliation after uncertainty or fence turnover
  remains a separate workstream.
- The mutation-authority/catalogue seam and its conformance contract are part
  of this completed slice. A production linearizable database, catalogue,
  lease service, and launcher admission implementation remain deferred.
- The implementation is limited to an approved local filesystem and manual
  fencing. NFS or another shared backend needs server-side stale-writer
  fencing, idempotency, publication, and fault evidence.
- Git Summary remains deferred user context and is not checkpoint or restore
  authority.

## Next Steps

- Add replay-only uncertain-result reconciliation, then verify same-image
  resume and rollout-tail repair.
- Implement an ext4 or filesystem-image backend before differential export,
  content-addressed storage, retention, and cross-host migration verification.
- Replace the conformance mutation authority with a production linearizable
  reservation, catalogue, fence, and launcher-admission implementation.

## Evidence

- `docs/architecture/stopped-directory-backend.md`
- `src/stopped-directory-backend.mjs`
- `test/stopped-directory-backend.test.mjs`
- PR #11
