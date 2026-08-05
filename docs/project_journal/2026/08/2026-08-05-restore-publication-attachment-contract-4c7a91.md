---
id: 20260805-4c7a91
title: Restore Publication Attachment Contract Correction
status: completed
created: 2026-08-05
updated: 2026-08-05
branch: wip/revert-broken-restore-composition
pr:
supersedes:
  - 20260804-93b7d2
superseded_by:
---

# Restore Publication Attachment Contract Correction

## Summary

Reverted the standalone restore publication-to-launch composition merged by
PR #27 after a frozen-range review exposed an unsatisfiable physical contract.
The facade required a fresh restore publication destination to equal the
canonical session attachment root. A conforming `ATTACHED` root is an existing
provider-backed directory, while stopped-directory restore publication requires
an absent final pathname that is not an active attachment. The passing fixture
had represented an absent pathname as an attached root and therefore did not
exercise a legal storage state.

## Decision

- Preserve PR #26's durable atomic generation-to-prepared-launch handoff and
  logical launcher foundations.
- Preserve the current-state description of PR #26's migration version 3
  operation-ID registry; the facade revert does not remove that retained
  schema or its dispatch trigger.
- Remove PR #27's facade and callback-contract changes instead of leaving a
  large fresh path unreachable or retaining synthetic authorization tests.
- Keep production restore unavailable; no enabled production path regressed.
- Add an explicit prerequisite: publish into an independent detached
  destination, verify its committed object identity, obtain provider-backed
  attachment evidence, and atomically activate that new attachment for the
  session and prepared launch.
- Stop, fence, and detach the old attachment before replacement. Exact path
  equality may bind metadata but cannot prove filesystem object identity or
  attachment authority.

## Protected Property

The protected property is that the writer launched after restore binds the
exact object durably published by that restore, under one current canonical
attachment authority. An existing attachment path and an absent publication
destination cannot be the same object at fresh dispatch. Removing only their
equality check would instead permit publication to one directory and launch
from another, so the incomplete composition remains removed until a typed
activation transition closes that boundary.

## Evidence

- `docs/architecture/session-storage-contracts.md` defines
  `attachment.rootPath` as the provider-backed host directory bound into the
  worker after attachment validation.
- `docs/architecture/stopped-directory-publication.md` requires the restore
  final pathname to be absent, outside worker admission, and not an active
  attachment before the no-replace rename.
- PR #27's composition fixture created only the destination parent and stored
  the absent child path in a synthetic `ATTACHED` document; its memory launcher
  did not inspect, pin, or bind that path.
- The production checkpoint authority continued to return
  `postgres_checkpoint_restore_unavailable`, so reverting the standalone
  facade restores the last reviewed production-neutral baseline.

## Next Steps

1. Complete durable writer-stop and capture composition.
2. Define the detached-destination attachment activation transition and its
   crash/replay contract.
3. Reintroduce publication-to-launch composition only after the handoff binds
   the provider-backed restored attachment atomically.
4. Add a filesystem-backed conformance test with a real existing old
   attachment and a distinct absent restore final pathname.

## Non-Goals

- No production restore enablement.
- No path-only substitute for storage-provider attachment proof.
- No concrete Podman, Docker, filesystem-image, or differential-export
  backend.
- No Git Summary implementation.
