---
id: 20260702-3e8a71
title: Snapshot and Restore Core
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/snapshot-restore-core
pr:
supersedes: []
superseded_by:
---

# Snapshot and Restore Core

## Summary

- Added backend-neutral orchestration for stopped-writer `clean` checkpoint
  capture and restore without selecting a physical storage implementation.
- Kept atomic canonical fence checks, storage barriers, durable idempotency,
  and physical capture or restore inside the backend trust boundary.

## Current State

- The core validates the manifest, storage, attachment, lease, and operation
  request before backend dispatch and constructs a portable descriptor only
  from a definite successful result.
- Restore requires a canonical target writer epoch strictly greater than the
  checkpoint source epoch and permits a replacement storage ID on the same
  backend.
- Every outcome that may have failed after backend dispatch remains uncertain
  and fails closed.
- Capture requires an opaque stopped-writer evidence handle that the backend,
  not the core, must authenticate and atomically bind to the mutation.
- Backend success must echo the complete dispatched checkpoint descriptor, so
  an operation-ID replay cannot combine an old physical mutation with new
  descriptor metadata.
- Writer stop and quiescence evidence, worker launch, Codex resume, concrete
  backends, tail repair, differential export, periodic snapshots, and
  cross-host verification remain outside this completed workstream. The
  optional committed-only API added by
  `2026-07-14-capture-reconciliation-91eac4.md` now reconciles exact durable
  results after lease expiry or fence turnover.

## Next Steps

- Verify same-image resume and rollout-tail repair before implementing an ext4
  or filesystem-image backend and differential export.

## Evidence

- `docs/architecture/snapshot-restore-core.md`
- `docs/architecture/session-storage-contracts.md`
- `src/session-snapshot-core.mjs`
- `test/session-snapshot-core.test.mjs`
