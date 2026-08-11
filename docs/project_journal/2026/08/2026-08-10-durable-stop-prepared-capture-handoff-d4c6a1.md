---
id: 20260810-d4c6a1
title: Durable Stop to Prepared Capture Handoff
status: completed
created: 2026-08-10
updated: 2026-08-10
branch: wip/durable-stop-prepared-capture-handoff
pr:
supersedes: []
superseded_by:
---

# Durable Stop to Prepared Capture Handoff

## Summary

Closed the restart gap between a durable physical writer stop and first
checkpoint-publication admission. Writer-stop request version 3 now carries an
exact clean-capture intent. One PostgreSQL transaction commits the stop,
releases its reservation, and materializes that exact checkpoint capture as a
prepared operation and reservation. Production `runRestore()` remains
fail-closed.

## Current State

- Migration 006 extends the permanent operation-ID registry with a
  `writer-stop-capture-intent-v3` claim and database triggers that require the
  capture ID to be reserved before stop dispatch and materialized only by the
  matching atomic handoff.
- Fresh V3 reservation is independently default-denied by
  `writerLaunchStopV3FleetCompatible`. Exact existing replay and recovery do
  not depend on the startup decision after durable work exists.
- `finalizeWriterLaunchStoppedAndReserveCheckpointCapture()` commits the
  complete-stop proof and the prepared capture in one serializable transaction.
  Its receipt and `reconcileWriterLaunchStopOperation()` readback carry the
  exact stop/capture relation.
- The launcher exposes `stopWriterForPreparedCapture()` without returning the
  opaque one-use stopped-writer capability. Legacy V1/V2 capture remains
  unchanged. The V3 local writer record cannot use
  `retireStoppedWriter()`; only `retirePreparedCapture()` with the exact
  predetermined committed capture result releases same-process launch
  exclusion.
- `runPreparedCapture()` re-reads the prepared authority tuple under the
  per-operation guard and calls fresh publication only after one definite
  prepared-to-starting dispatch grant. Claim acknowledgement uncertainty,
  false replay, or stale state performs zero publication.
- The optional storage extension
  `preparedCheckpointCaptureContractVersion: 1` uses the authority-issued
  attempt binding and `publishFreshCheckpointArtifact()`. It never adopts an
  existing prepared, materialized, or committed filesystem-journal phase.
- Checkpoint recovery now routes new handoff `prepared` candidates to the
  fresh-only resume path. `starting` and `uncertain` remain committed-only and
  source-free; they never call replay-capable publication.

## Safety Decisions

- The protected durable property is uninterrupted blocker identity. The
  capture operation ID is preclaimed before physical stop, and the final stop
  plus prepared capture share one database transaction, so a committed stop
  cannot leave the session with neither a launch nor a discoverable capture.
- The protected local access-policy property is same-session launch exclusion.
  V3 revokes the opaque snapshot capability but retains the coordinator record
  until the exact predetermined durable capture result is validated. A
  structurally valid but different checkpoint proof cannot release that
  record.
- The prepared callback's lease may be expired. It is historical fence
  correlation, not new writer authority. The cold path validates only the
  immutable backend/storage/session/lease/holder/epoch tuple; the definite
  durable capture dispatch grant authorizes publication.
- A committed capture finalizer whose acknowledgement is lost is recovered by
  exact committed readback and returns the original fresh completion by
  identity. It does not publish again or weaken the backend callback contract.
- A retained V3 stopped record caches the exact frozen atomic handoff receipt.
  An exact retry replays that receipt under the same operation guard without a
  second authority transition or physical stop. Only a truly `prepared`
  operation may reach the fresh publication callback; a stale prepared receipt
  is rejected by authority before that callback and falls back to source-free
  committed verification. The exact predetermined result is revalidated before
  local retirement.
- If capture claim commits but the process dies before publication, or if the
  filesystem journal is noncommitted after publication begins, recovery stays
  pending. Reacquiring an advisory guard does not prove the old callback
  quiesced and therefore cannot authorize a second fresh publication.

## Next Steps

1. Add the cross-process shared/exclusive restore lifecycle guard so foreground
   prepared launch cannot race recovery cancellation.
2. Add the bounded production recovery scheduler under the exclusive lease.
3. Behind the separate detached-production invocation gate, compose
   publication, this stop/capture handoff, detach, activation, prepared launch,
   and recovery through the checkpoint adapter before enabling `runRestore()`.

## Non-Goals

- No production restore entry point or detached-production fleet capability.
- No cross-process restore lifecycle scheduler or guard.
- No second-publication lease or takeover protocol for an ambiguous fresh
  callback.
- No concrete Podman, Docker, filesystem-image, NFS, or cross-host backend.
- No Git Summary implementation.

## Evidence

- `migrations/authority/006-writer-stop-capture-handoff.sql`
- `src/postgres-session-authority.mjs`
- `src/postgres-logical-writer-launcher.mjs`
- `src/postgres-durable-stop-capture-composition.mjs`
- `src/postgres-checkpoint-mutation-authority.mjs`
- `src/postgres-checkpoint-recovery-service.mjs`
- `src/stopped-directory-backend.mjs`
- `integration/postgres-session-authority.mjs`
- Focused launcher, composition, mutation-authority, recovery-service,
  storage-contract, stopped-backend, snapshot-core, migration, and operation
  kernel tests.
- Real PostgreSQL execution remains a required CI gate because
  `SESSION_AUTHORITY_DATABASE_URL` is unavailable in this workspace.
