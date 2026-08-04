---
id: 20260804-5a7c2e
title: Atomic Restore-to-Launch Handoff
status: completed
created: 2026-08-04
updated: 2026-08-04
branch: wip/restore-launch-handoff
pr: https://github.com/cha-op/portable-codex-runtime/pull/26
supersedes: []
superseded_by:
---

# Atomic Restore-to-Launch Handoff

## Summary

Closed the durable crash gap between committing one restore destination
generation and reserving its logical writer launch. Restore request version 2
binds an exact launch intent and durably claims its globally unique operation
ID before publication. One later serializable PostgreSQL transition commits
the generation, materializes that claim as the prepared launch attempt, and
the launcher consumes only the pre-reserved attempt with an exact opaque image
capability.

## Current State

- `createRestoreDestinationGenerationOperationRequestV2()` records the exact
  launch-attempt ID, measured-image projection, and supervisor identity in the
  restore operation request. Existing version 1 requests remain valid but have
  no launch intent.
- Migration version 3 adds the permanent
  `session_authority.operation_id_registry`. Ordinary operations claim and
  materialize their ID together; version 2 restore dispatch instead records an
  unmaterialized `restore-launch-intent-v2` claim bound to the claimant restore
  operation before granting publication.
- `finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt()` commits
  the authorized generation, restore operation, and released reservation;
  writes the restore terminal session revision; derives the existing
  `writer-launch-attempt-v1` request from that exact snapshot; validates and
  materializes the pre-publication claim; reserves it; and writes the
  prepared-launch session revision in one serializable transaction.
- Exact replay returns the same generation and launch attempt. Intent changes,
  registry ownership conflicts, revision exhaustion, standalone-finalize
  gaps, and partial-state conflicts fail closed instead of fabricating a
  missing launch reservation.
- Restore-generation recovery candidates expose the exact durable launch
  intent for version 2 while preserving the historical three-field version 1
  candidate shape, without treating either form as an opaque image capability.
- `prepareLaunchIntent()` revalidates but does not consume the original image
  reservation. `runPreparedLaunch()` reads and claims only the exact existing
  prepared attempt; it does not reserve another operation.
- After a restart, a trusted resolver may mint a fresh opaque reservation only
  while exact authority readback still reports that attempt as `prepared`; its
  complete measurement must match the durable launch intent before claim.
- The prepared launcher consumes the image reservation and invokes the
  supervisor only after definite durable claim. Starting or uncertain state
  follows no-relaunch reconciliation, committed state follows exact readback,
  and a prepared mismatch leaves both the durable attempt and process-local
  capability untouched. A claim failure whose readback remains prepared also
  leaves the attempt retryable instead of cancelling required launch work.
- A granted claim receipt must preserve the previously read expected-session
  content, exact active-pointer revision transition, operation/reservation
  timestamps, and an authority clock still inside the bound lease. Hostile
  lifecycle, co-mutated session, or clock data falls back to durable readback
  without image consumption or supervisor launch.

## Safety Decisions

- The protected property is one-to-one durable continuity from the authorized
  restore generation to its intended launch attempt, including permanent
  non-reuse of the launch-attempt ID. The ID is therefore durably claimed
  before external publication, while generation commit, registry
  materialization, and launch reservation share one later transaction; a
  read-only existence check or two separately atomic calls are insufficient.
- The first session revision records the committed restore terminal anchor.
  The launch request is derived from that exact snapshot, and the second
  revision records its active prepared pointer. Neither intermediate state is
  externally visible before commit.
- Before either handoff write, the authority reserves a five-revision budget:
  two revisions for the atomic handoff and three for the prepared attempt's
  claim, uncertainty, and terminal lifecycle. An attempt that cannot complete
  that lifecycle is rejected before any generation or session mutation.
- Before version 2 restore dispatch authorizes physical publication, it
  reserves the complete seven-revision worst case: restore claim and
  uncertainty, both handoff writes, then launch claim, uncertainty, and
  terminal completion. Version 1 keeps its three-revision restore boundary.
- Prepared cancellation reconstructs the exact version 2 handoff provenance
  from the terminal restore request, committed generation, launch intent, and
  same-transaction operation records. A matching atomic handoff cannot be
  retired by generic reconciliation; version 1 prepared work remains
  cancellable.
- The serialized launch intent is only correlation and recovery data. It does
  not recreate the original image reservation, consume it, or authorize a
  process launch by itself.
- Version 1 compatibility does not silently upgrade an already committed
  generation into launch authority. Production composition must use an exact
  version 2 request before physical publication begins.
- Version 1 requests neither create nor require a launch-intent registry claim,
  and retain their historical finalisation and recovery shapes. The migration
  backfills existing ordinary operations as materialized `direct-operation`
  claims without changing their lifecycle.
- An old version 2 restore operation may gain the new launch-intent claim only
  while it is still `prepared`, through its next upgraded dispatch before
  publication. Migration drains in-flight legacy operation writers with an
  `ACCESS EXCLUSIVE` table lock, then installs a trigger that rejects any
  post-upgrade old-binary transition without the exact claim while preserving
  the strict `prepared -> committed/cancelled-before-dispatch` escape path.
  Migration
  aborts with SQLSTATE `55000` on old `starting`,
  `uncertain`, or `committed` version 2 work because neither a later row
  insertion nor a completed operation can prove pre-publication ownership.
  Operators must drain or quarantine that state first.
- Version 2 remains dormant until production composition lands. Its rollout
  must upgrade every authority/recovery node sharing the database before an
  explicit fleet-capability gate allows any node to create version 2 work;
  version 1 recovery candidates keep their historical exact shape.
- The registry is an identity relation, not a second launch state machine.
  Its rows are permanent, and `materialized_at` records the one transition from
  a restore-bound launch intent into the ordinary operation lifecycle.

## Next Steps

1. Verify committed restore publication and compose it with the atomic handoff
   and prepared launcher without treating a published path as launch authority.
2. Route exact coordinator stop confirmation through
   `writer-launch-stop-v1`, retain the required local stopped-writer relation,
   and bind its durable proof to capture admission.
3. Add bounded generation and launch recovery services, then enable
   production `runRestore()` only after the complete protocol remains
   fail-closed across acknowledgement loss and restart.
4. Implement the later filesystem-image backend, differential export,
   retention, and cross-host recovery verification.

## Non-Goals

- No production `runRestore()` enablement.
- No publication verifier or production restore adapter.
- No complete durable stop/capture composition or operational recovery loop.
- No concrete Podman or Docker launcher.
- No filesystem-image backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-session-authority.mjs`
- `migrations/authority/003-operation-id-registry.sql`
- `src/postgres-logical-writer-launcher.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/postgres-logical-writer-launcher.test.mjs`
- `integration/postgres-session-authority.mjs`
- `createRestoreDestinationGenerationOperationRequestV2()`
- `PostgresSessionAuthority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt()`
- `createPostgresLogicalWriterLauncher()`
