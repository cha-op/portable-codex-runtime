---
id: 20260729-f3c8a1
title: Durable Operation and Reservation Kernel
status: completed
created: 2026-07-29
updated: 2026-07-30
branch: wip/session-operation-reservation
pr:
supersedes: []
superseded_by:
---

# Durable Operation and Reservation Kernel

## Summary

- Added the durable pre-dispatch phase boundary between canonical session
  registration and later writer lifecycle operations.
- Kept lease allocation, attachment, provider callbacks, physical fencing, and
  business-specific success finalization outside this slice.

## Current State

- One conservative `session-mutation` conflict class serializes all
  authority-changing operations for a session. `kind` does not weaken that
  exclusion.
- An exact operation request binds the complete caller-observed session
  snapshot, operation ID, kind, bounded canonical request, request digest, and
  one authority-owned reservation. Canonicalization enforces the JSON byte and
  structure budgets incrementally before sorting, copying, or serializing the
  accepted request.
- Reserve atomically claims the operation and reservation, writes the matching
  session `activeOperation` pointer, and increments the session revision.
- Dispatch admission is a separate durable `prepared -> starting` CAS. Only
  the call that definitely commits that transition receives a dispatch grant;
  replay, restart, or commit uncertainty cannot grant a second dispatch.
  PostgreSQL integration coverage includes a COMMIT that lands before its
  acknowledgement is synthetically lost, followed by reconcile and replay.
- `starting -> uncertain` retains the operation and reservation as an active
  blocker. No timeout, process restart, or lease observation releases it.
- A still-`prepared` operation can be cancelled before dispatch. Cancellation
  records one exact terminal result, releases the reservation, clears the
  session pointer, and remains permanently replayable under the same operation
  ID.
- Strict readback cross-validates the canonical session pointer with the
  operation and reservation rows. Missing, partial, malformed, or mismatched
  state fails closed without repair.
- All phase changes use database time and exact revision CAS. Exact replays do
  not rewrite revisions or timestamps.

## Next Steps

- Implement database-clock writer lease acquisition and renewal plus exact
  attachment finalization.
- Reuse the kernel's durable `starting` blocker, but finalize lease and
  attachment state in the same transaction that retires the operation and
  releases its reservation.
- Preserve unresolved provider or finalization outcomes for typed
  reconciliation; never use a generic release as physical-fence evidence.

## Evidence

- `src/postgres-session-authority.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
- `docs/architecture/runtime-delivery-plan.md`
