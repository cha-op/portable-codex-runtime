---
id: 20260829-d7f3c1
title: Physical-Fence Capture Handoff Foundation
status: completed
created: 2026-08-29
updated: 2026-08-29
branch: wip/atomic-crash-capture-physical-fence
pr:
supersedes: []
superseded_by:
---

# Physical-Fence Capture Handoff Foundation

## Summary

- Added a durable force-fence V2 handoff whose
  `writer-fence-atomic-capture-intent-v2` claim preclaims one independent
  `atomic-crash-capture-v1` session operation before external fence dispatch.
- Made exact physical-fence finalization and prepared-capture materialization
  one serializable transaction, leaving the capture as the active
  session-conflicting reservation after the old writer becomes `DETACHED`.
- Added exact terminal replay and restart readback for that fixed handoff. The
  slice intentionally stops before snapshot dispatch and retains the blocker
  without a release path.

## Current State

- The permanent capture-operation claim binds the same session, claimant
  force-fence operation, and complete immutable
  `atomicCapture: { operationId, request }` intent before a provider may
  receive dispatch.
- Only the exact trusted `status: "fenced"` result for the dispatched request
  may commit the force-fence transition. The same transaction materializes the
  capture operation and reservation in `prepared`, then makes it the session's
  active pointer.
- Migration 013 enforces the reverse relation at commit time: a successful V2
  fence must retain the exact prepared capture operation and reservation,
  released fence reservation, and matching `DETACHED` session pointers.
- The V2 fence identity and terminal row and the prepared capture operation are
  immutable in this foundation. Claim, reservation, and session mutations each
  schedule the same deferred reverse check, preventing later direct SQL
  removal or detachment of the blocker while the successful fence remains.
- The canonical lifecycle is `DETACHED` after the old attachment is physically
  fenced, but successor admission remains blocked by the active prepared
  capture across process loss, acknowledgement loss, and restart.
- Exact replay and
  `reconcileWriterForceFenceAtomicCaptureHandoff()` return the same committed
  fence and prepared capture without another provider dispatch, capture
  creation, or blocker release.

## Safety Boundary

- The protected ordering property is a durable, gap-free handoff from one
  authenticated physical-fence proof to one predetermined capture blocker. A
  database epoch, lease expiry, or `FENCING`, `BLOCKED`, or `DETACHED` state is
  not physical exclusion evidence.
- This foundation does not supply a real stale-writer fence provider and does
  not prove host, controller, or drive cache loss. The current ext4 backend
  remains manual-fencing and public deployment remains unchanged.
- It does not dispatch or reconcile an LVM snapshot, repair a crash prefix,
  restore a writable generation, or admit a higher-epoch writer. Because this
  slice has no blocker-release transition, every committed handoff stays
  deliberately fail-closed.

## Follow-up

- Compose a concrete automatic fence provider and the prepared capture with
  exact provider and acknowledgement-loss recovery semantics.
- Release the blocker only after the exact atomic result is durably committed,
  then repair on a separate writable generation before higher-epoch writer
  admission and public recovery wiring.
- Keep cache-loss evidence and export, distribution, encryption, retention,
  registry trust, and remote transport separately scoped.

## Validation

- Relevant source, unit-test, and integration modules passed `node --check`.
- The nine focused force-fence V2 cases passed. The complete session
  operation-kernel suite passed 295/295, the authority suite passed 34/34, the
  PostgreSQL serializable-store suite passed 152/152, and the detached-runtime
  suite passed 24/24.
- `git diff --check` passed for the related implementation, integration, test,
  and documentation changes, and project-journal validation passed.
- Fresh-context local Codex review found that the original reverse constraint
  could be bypassed by later direct SQL identity or relation changes. The fix
  makes the V2 fence terminal and capture blocker immutable and schedules the
  same deferred session-scoped check from operation, registry, reservation,
  and session mutations; real-PostgreSQL coverage now exercises each tamper
  class.
- The full Node.js 24.18.0 suite passed with the unchanged
  `chatgptAuthTokens refreshes after 401 without writing auth.json` case
  excluded through `--test-skip-pattern`. That case independently reproduced
  the host-level `EMFILE: too many open files, watch` baseline; neither its
  source nor its test differs from `master`.
- `npm run test:postgres` reached the repository's required environment gate
  but could not run the real PostgreSQL scenario because
  `SESSION_AUTHORITY_DATABASE_URL` is not configured locally. The Ubuntu
  PostgreSQL CI job remains the required execution evidence for migration 013,
  its deferred terminal-blocker constraint, and the same-transaction handoff.

## Evidence

- Migration: `migrations/authority/013-writer-fence-atomic-capture-handoff.sql`
- Authority: `src/postgres-session-authority.mjs`
- Contracts: `src/session-storage-contracts.mjs`
- Unit coverage: `test/postgres-session-operation-kernel.test.mjs`
- PostgreSQL integration: `integration/postgres-session-authority.mjs`
- Architecture: `docs/architecture/session-runtime-authority.md`
- Crash-capture boundary: `docs/architecture/atomic-crash-capture-extension.md`
