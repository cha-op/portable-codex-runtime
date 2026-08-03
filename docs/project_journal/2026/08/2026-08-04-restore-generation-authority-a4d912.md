---
id: 20260804-a4d912
title: Typed Restore Generation Authority
status: completed
created: 2026-08-04
updated: 2026-08-04
branch: wip/restore-generation-authority
pr:
supersedes: []
superseded_by:
---

# Typed Restore Generation Authority

## Summary

Added the typed PostgreSQL state machine that reserves, authorises, finalises,
reads, and recovers one canonical restore destination generation without
enabling production restore or launcher admission.

## Current State

- The typed operation request retains the backend's exact
  `{ checkpoint, request }` admission and a predetermined restore result.
- The operation envelope continues to bind the complete expected session,
  including its current lease, attachment, storage reference, revision, and
  terminal anchor.
- A serializable claim locks and revalidates the session, operation,
  reservation, committed checkpoint catalogue, and destination-generation
  relation before granting one restore publication dispatch.
- The claim checks the database clock, exact current destination lease and
  storage reference, a strictly newer restore fence, and the complete source
  checkpoint relation.
- Fresh generation and destination-isolation proof identities are supplied
  only at the typed claim boundary and become immutable members of the
  generation binding.
- An `authorized` generation has no document or commit timestamp. Exact
  finalisation atomically writes the committed restore document, retires the
  operation, releases its reservation, and advances the session terminal
  anchor.
- Committed replay requires the same canonical generation document.
  Acknowledgement loss after claim never grants a second dispatch, while an
  exact finalisation replay returns the retained committed generation.
- Bounded keyset recovery enumerates only retained `starting` or `uncertain`
  restore-generation operations with an exact authorised generation.

## Safety Decisions

- The canonical session remains `ATTACHED`; the generation binding's
  `destinationState: "detached"` describes the isolated physical restore
  target, not the session lifecycle.
- The source checkpoint and destination mutation must name the same backend
  and session, but the checkpoint's source storage identity is not inferred
  from the destination request.
- Publication remains outside every PostgreSQL transaction. The authority
  persists dispatch and result boundaries but does not perform filesystem
  mutation.
- A destination-isolation proof ID is a durable correlation value, not a
  self-authenticating capability. Later composition must obtain it from the
  trusted destination authority.
- A committed generation is durable input for future launcher admission; it is
  not writable-launch authority by itself.
- No migration version 3 or canonical session document version 3 is required
  for this slice.
- The production checkpoint adapter remains capture-only and `runRestore()`
  remains fail-closed.

## Remaining Work

1. Add the durable launch-attempt lifecycle and bind a finalised generation,
   exact attachment and lease, image measurement, process incarnation, writer
   incarnation, and supervisor evidence.
2. Compose one-use measured-image consumption, durable launch dispatch,
   external launcher execution, and exact writer registration.
3. Enable production restore only after acknowledgement-loss and restart
   behaviour preserve the no-second-writer boundary.
4. Implement the later filesystem-image backend, differential export,
   retention, and cross-host recovery verification.

## Non-Goals

- No restore publication callback in a database transaction.
- No production restore enablement.
- No launcher or supervisor call.
- No Podman or Docker execution.
- No filesystem-image backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-session-authority.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `integration/postgres-session-authority.mjs`
- `README.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
