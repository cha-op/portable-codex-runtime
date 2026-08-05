---
id: 20260804-93b7d2
title: Restore Publication-to-Launch Composition
status: completed
created: 2026-08-04
updated: 2026-08-04
branch: wip/restore-publication-launch-composition
pr:
supersedes: []
superseded_by:
---

# Restore Publication-to-Launch Composition

## Summary

Added a production-neutral composition facade (a standalone implementation not
wired into the production checkpoint adapter) for the ordered restore path:
typed durable read, fresh-only fleet gate, version 2 claim, exact physical
publication, independent committed verification, atomic handoff, then prepared
launch. Restore callback contract version 3 now carries the full typed
generation binding unchanged through the stopped-directory backend and
publication journal, while production `runRestore()` remains intentionally
unavailable.

## Current State

- `PostgresSessionAuthority` accepts
  `restoreLaunchV2FleetCompatible: true` only as an explicit startup decision.
  The default is false. The check occurs only after exact operation replay
  lookup and therefore blocks fresh version 2 reservation without blocking
  existing replay, finalisation, or recovery.
- `createPostgresRestorePublicationLaunchComposition()` first performs an
  exact typed durable read. Only an absent operation requires its
  invocation-time `fleetCapabilityGate()` to return the frozen
  `RESTORE_LAUNCH_V2_FLEET_CONFIRMED` sentinel before restore preparation or
  durable creation; exact replay bypasses that fresh-work gate.
- The facade prepares the isolated destination, opaque image reservation, and
  durable launch intent before taking the restore operation guard. Under the
  guard it re-reads the exact request, reserves or claims only when required,
  calls the backend publisher once, independently verifies the committed
  publication through the bound real publication instance, and confirms the
  atomic handoff. Keeping preparation outside the guard avoids shared-pool
  starvation while retaining the publication-sensitive serialization
  boundary.
- Physical publication receives the authority-returned full generation
  binding plus the exact artefact, lease, storage, destination, path, and
  predetermined-result context. Its coordinator-binding digest must match the
  same generation binding before handoff.
- Restore callback contract version 3 requires and validates that full
  generation binding plus an explicit publication mode, then passes the same
  frozen object unchanged to the publication journal. A legacy version 2
  callback has neither field; the backend derives its historical four-field
  version 1 journal binding internally and permits only committed read-only
  verification. It cannot authorize a fresh publication, resume a
  non-committed legacy publication, or replace typed generation identity.
- Handoff acknowledgement loss replays only the same handoff under the held
  guard, after a fresh ownership assertion. The complete handoff receipt is
  bound back to both durable operations and reservations, the generation and
  catalogue, the exact launch request, and the canonical session relation.
  After handoff confirmation, the guard is released and
  `runPreparedLaunch()` receives the original opaque reservation through the
  same frozen shallow wrapper snapshot used to prepare the launch, plus the
  exact pre-reserved launch-attempt ID; it does not reserve another operation.
  Its `started` result is accepted only when the full terminal
  operation/reservation/session/evidence relation validates and the durable
  revision is exactly 2 after prepared/starting, exactly 3 after uncertain, or
  unchanged for committed replay. Stable session identity—including its
  manifest, storage, backend capabilities, attachment, lease, lifecycle,
  recovery state, writer epoch, document version, creation time, and session
  ID—must still match the atomic handoff while operation-owned pointers,
  history, revision, launch state, and update time follow their own transition
  contracts.
- A supported version 2 expected session remains unchanged inside the durable
  operation request while the authority upgrades its current active and
  terminal session receipts to document version 3. Those receipts must match
  the complete expected document after applying only that version upgrade and
  the authority-owned active-operation pointer. Version 1 restore sessions
  remain unsupported and fail before fleet gating or durable work.
- A failure before definite publication dispatch may cancel only the prepared
  restore. A failure after dispatch but before confirmed handoff leaves or
  marks durable uncertainty. A launch failure after handoff remains owned by
  the existing no-relaunch reconciliation path.
- `createPostgresCheckpointMutationAuthority().runRestore()` is unchanged and
  still fails with `postgres_checkpoint_restore_unavailable` without invoking
  its publication callback.

## Safety Decisions

- The protected property is an unbroken one-to-one chain from the fresh
  version 2 restore operation, through its authority-owned generation binding
  and physical publication evidence, to its permanently registered launch
  attempt. A path, journal row, generation row, serialized image measurement,
  or launch ID alone is insufficient authority.
- Startup fleet compatibility controls only new durable request creation.
  Replay and recovery must stay available even if rollout policy later closes
  the gate; otherwise disabling admission would strand already-authorized
  work.
- The typed durable read runs before the invocation-time fleet gate. The gate
  runs only for absent work and before preparation or database writes, while
  the authority option remains the final fresh-reservation enforcement
  boundary. Neither gate is inferred from a request field or serialized
  capability claim.
- Preparation remains outside both PostgreSQL transactions and the advisory
  guard. Publication and its independent committed-state verification remain
  outside transactions but inside the exact advisory guard. The atomic handoff
  is confirmed before the image reservation can be consumed or the external
  launcher invoked.
- The full generation binding is passed by object identity, not reconstructed
  from a reduced journal projection. Legacy version 1 compatibility is
  read-only and committed-only, so it cannot fabricate the coordinator digest
  required by a fresh typed version 2 generation.
- The mutable preparation result is never retained as the image capability
  wrapper. Its four own data fields are sampled once into a frozen wrapper,
  preserving the opaque inner reservation identity while preventing a later
  outer-property swap between launch preparation and execution.
- Session comparisons protect content stability, not JavaScript object
  identity. A legal version 2-to-3 authority upgrade is accepted only at the
  current-session boundary; every unrelated stable field is reconstructed from
  the expected session and compared as one canonical document. The terminal
  launcher result independently binds the same stable identity back to the
  handoff receipt.
- The standalone facade does not provide durable complete-stop proof, capture
  admission, bounded operational recovery, a concrete container driver, or
  production adapter enablement.

## Next Steps

1. Route exact coordinator stop confirmation through
   `writer-launch-stop-v1`, retain only same-process capability state that can
   still prove writer identity, and join durable complete-stop proof to later
   capture admission.
2. Compose bounded generation, prepared-launch, active-attempt, and
   current-launch recovery without relaunching or reconstructing opaque image
   or writer capabilities.
3. Wire the complete fail-closed protocol into the production checkpoint
   authority's `runRestore()` only after stop/capture and recovery behavior is
   verified across acknowledgement loss and restart.
4. Implement the later filesystem-image backend, differential export,
   retention, and cross-host recovery verification.

## Non-Goals

- No production `runRestore()` enablement.
- No durable stop-to-capture composition.
- No bounded operational restore or launch recovery service.
- No concrete Podman or Docker launcher.
- No filesystem-image backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-restore-publication-launch-composition.mjs`
- `src/postgres-session-authority.mjs`
- `src/stopped-directory-backend.mjs`
- `src/postgres-checkpoint-mutation-authority.mjs`
- `test/postgres-restore-publication-launch-composition.test.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/stopped-directory-backend.test.mjs`
- `test/postgres-checkpoint-mutation-authority.test.mjs`
- `integration/postgres-session-authority.mjs`
- `createPostgresRestorePublicationLaunchComposition()`
- `RESTORE_LAUNCH_V2_FLEET_CONFIRMED`
