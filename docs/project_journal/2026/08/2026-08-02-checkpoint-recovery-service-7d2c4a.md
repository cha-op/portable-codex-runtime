---
id: 20260802-7d2c4a
title: Bounded Checkpoint Recovery Service
status: completed
created: 2026-08-02
updated: 2026-08-02
branch: wip/checkpoint-recovery-service
pr:
supersedes: []
superseded_by:
---

# Bounded Checkpoint Recovery Service

## Summary

Added one bounded operational path that discovers retained PostgreSQL
`checkpoint-capture-v1` operations in `starting` or `uncertain`, reconstructs
only their exact durable checkpoint and mutation request, and submits each
candidate to the existing source-free committed-artefact reconciliation path.

## Current State

- `PostgresSessionAuthority.listCheckpointCaptureRecoveryCandidates()` is a
  read-only page API with an exact
  `{afterSessionId, limit}` request.
- It scans only active `starting` or `uncertain` checkpoint operations using
  `session_id` keyset order and a hard `limit + 1` query bound.
- Every returned operation is revalidated against the current session pointer,
  reservation, capture-attempt binding, tombstone absence, and catalogue
  absence in the same serializable snapshot.
- Each candidate is an exact frozen `{checkpoint, request}` admission recovered
  from the canonical durable operation envelope. No current session, source,
  lease, attachment, or stopped-writer input participates in reconstruction.
- `createPostgresCheckpointRecoveryService()` accepts fixed candidate-list and
  committed-reconciliation collaborators and exposes exact
  `runBatch({afterSessionId, limit, signal})` admission.
- One batch lists one bounded page and processes it sequentially. Each settled
  item records its operation and session IDs plus `reconciled` or `pending`;
  the batch reports `sweep-complete`, `limit-reached`, or `aborted` with the
  non-null continuation cursor after the last settled item, or null after a
  completed sweep.
- One service instance admits only one batch at a time. An overlapping valid
  invocation fails closed before enumeration or reconciliation and can retry
  after the in-flight batch drains.
- A pending sanitized reconciliation outcome does not prevent later admitted
  candidates in the same page from being attempted.
- The in-memory cursor advances only after the current candidate settles.
  Completing a sweep returns a null cursor, so the next scheduled pass starts
  at the beginning and can recover candidates created behind the prior cursor.
- An already-aborted signal admits no work. Aborting during a batch stops
  admission of the next candidate, but the in-flight reconciliation is awaited
  through guard release before the batch returns; cancellation never races it
  with `Promise.race` or detaches it.
- The backend and stable artefact-root resolver are constructed once from
  copied, frozen startup configuration before their reconciliation collaborator
  is installed in the service.
- This slice reuses the existing active-operation partial index and version 1
  authority schema without DDL.
- Deterministic unit and real-PostgreSQL coverage exercise pagination, state
  filtering, exact durable admissions, sequential receipts, cursor replay,
  abort admission, guard-busy pending work, retry, and terminalization.

## Safety Decisions

- `session_id` is the cursor because it is immutable for the operation and the
  existing active-operation partial index is ordered by it. Mutable
  `updated_at` is not a cursor.
- A cursor is only a fairness/progress hint. It is not an authority token,
  exactly-once claim, or cross-page snapshot. A candidate may disappear,
  finalize, or replay between enumeration and reconciliation.
- Enumeration never accepts caller-provided session, attachment, lease, source
  path, stopped-writer capability, or reconstructed publication state.
- Tombstoned, catalogued, missing, malformed, or cross-bound durable relations
  fail the page closed instead of being silently excluded by SQL.
- Guard-busy work remains unchanged. An unverifiable `starting` operation may
  follow the existing safe transition to `uncertain`; both states retain the
  durable reservation and attempt for a later pass.
- Processing concurrency is fixed at one both within a page and across
  concurrent calls on one service instance. No detached promise, speculative
  cleanup, new capture attempt, normal capture callback, or publication retry
  is permitted.
- Abort or admission deadlines stop only new candidate work. An in-flight
  reconciliation must settle and release its guard before the batch returns;
  `Promise.race` is not a safe cancellation boundary.

## Explicit Limits And Dependencies

- Page and batch sizes are hard-bounded to at most 100 candidates.
- The current active-session partial index avoids scanning retired history,
  but result limits do not strictly bound PostgreSQL work when many other
  active operation kinds exist. Production still requires statement/request
  deadlines.
- The backend and its artefact-root resolver must be constructed once from
  copied, frozen startup configuration. A backend ID must not silently move to
  another artefact root.
- The current verifier has no cooperative cancellation seam, so this slice
  bounds admitted work and candidate count, not worst-case wall-clock time.
- Authority-ledger disaster recovery must remain monotonic relative to every
  retained artefact and session-volume generation.

## Non-Goals

- No schema or migration-runner change.
- No durable work queue, worker claim, persisted backoff, or exactly-once
  scheduler.
- No multi-backend dispatcher.
- No restore destination generation or launcher admission.
- No ext4/filesystem-image backend, differential compression, retention, or
  cross-host restore.
- No Git Summary implementation.

## Remaining Work

- Implement canonical restore destination generations and logical launcher
  admission while keeping production restore fail-closed until both authorities
  are composed.
- Implement an ext4 or filesystem-image physical backend and the later
  retention, differential, content-addressed, and cross-host recovery layers.
- Keep the read-only Git Summary separate from checkpoint correctness.

## Evidence

- `src/postgres-session-authority.mjs`
- `src/postgres-checkpoint-recovery-service.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/postgres-checkpoint-recovery-service.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
- `README.md`
- `docs/PROJECT_STATE.md`
- `docs/PROJECT_TODO.md`
