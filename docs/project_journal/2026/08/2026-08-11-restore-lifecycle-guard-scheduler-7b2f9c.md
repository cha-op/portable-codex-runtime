---
id: 20260811-7b2f9c
title: Restore Lifecycle Guard and Recovery Scheduler
status: completed
created: 2026-08-11
updated: 2026-08-11
branch: wip/restore-recovery-lifecycle-scheduler
pr:
supersedes: []
superseded_by:
---

# Restore Lifecycle Guard and Recovery Scheduler

## Summary

Added the database-global shared/exclusive lifecycle boundary required before
foreground production restore can coexist with scheduled no-relaunch recovery.
Foreground composition can hold a shared lease, while each bounded four-lane
recovery step holds the matching exclusive lease. A production scheduler now
runs that branded recovery runner immediately and at a fixed delay without
overlapping steps. Production `runRestore()` remains fail-closed.

## Current State

- `PostgresOperationGuard` supports both shared and exclusive PostgreSQL
  session advisory locks over the same versioned lock key. Its dedicated
  client, reset, health probe, unlock, and destruction rules apply to both
  modes. Pool acquisition and query submission use callback-only node-postgres
  adapters whose raw values are sealed inside private null-prototype carriers
  before Promise settlement. Promise-returning guard callbacks must fulfill
  with the exact per-run `complete(value)` carrier, so prototype or species
  poisoning cannot release a lock before the real callback drains.
- `PostgresRestoreLifecycleGuard` fixes one versioned lock identity for the
  complete candidate universe in an authoritative database. It mints opaque,
  callback-scoped foreground or recovery leases; structural lookalikes and
  stale leases cannot authorize a lifecycle action.
- The restore recovery runner requires a branded lifecycle guard and executes
  every four-lane pass under its exclusive recovery lease. It revalidates that
  lease around cursor reads, service batches, and durable cursor advances.
- Guarded recovery-service calls revalidate the same lease around listing and
  every admitted reconciliation callback. Existing direct unscheduled service
  calls retain their legacy shape, but the production runner always uses the
  guarded variant and binds each one-shot batch receipt to the exact lease.
- `PostgresRestoreRecoveryScheduler` starts with one immediate bounded pass,
  serializes later fixed-delay passes, coalesces concurrent kicks, reports
  busy and uncertain outcomes through one synchronous `undefined`-returning
  observer, rejects Promise/thenable observer returns, and drains an admitted
  pass before `stop()` settles.

## Safety Decisions

- The protected scheduling property is foreground/recovery admission
  exclusion over the full database candidate universe. Recovery cursor scopes
  do not partition authority candidates, so lifecycle lock identity is not
  derived from `recoveryScopeId`.
- The fixed lifecycle lock and ordinary per-operation locks use distinct
  versioned advisory-key namespaces. Shared and exclusive lifecycle modes keep
  one key, while an existing durable operation whose ID equals the lifecycle
  label remains in the ordinary namespace and cannot deadlock against its
  outer lifecycle lease.
- The exclusive lease spans list, reconcile, and cursor settlement. A cursor
  cannot advance from an unguarded or differently guarded batch receipt, and a
  later lane is not admitted after a failed lease probe.
- The scheduler uses one bounded runner pass per tick. `limit-reached` schedules
  another tick rather than creating an unbounded drain loop, and a tick that
  finds a foreground shared lease reports `busy` without invoking recovery.
- Shutdown aborts admission of new candidates, but does not race an already
  admitted callback. The service drains that callback and the runner persists
  its settled continuation before releasing the exclusive lease.
- A PostgreSQL connection loss releases its session advisory lock. Boundary
  probes detect that loss and fail closed, but they are cooperative checks and
  do not constitute durable provider fencing or prove that no instruction can
  execute between a successful probe and a later external side effect. The
  existing typed transitions and per-operation guard remain authoritative for
  physical dispatch.
- Guard, service, runner, and scheduler Promise boundaries use captured native
  reactions. Callback-time prototype poisoning cannot settle public work
  before its probes and cleanup drain, while a structurally protected Promise
  from another trusted module is adopted through a captured native bridge.
- Lifecycle callbacks receive the same operation-scoped `complete` capability
  as the underlying guard. Async recovery work returns that authentic carrier;
  only after the exclusive lock probe and cleanup drain does the facade expose
  the original recovery result.

## Next Steps

1. Add the separate invocation-time detached-production fleet capability.
2. Compose committed publication, durable stop and prepared capture, canonical
   detach, capture-bound activation, prepared launch, and this scheduled
   recovery runtime through the production checkpoint adapter.
3. Enable `runRestore()` only after the complete adapter passes restart and
   ambiguous-outcome coverage without a second writer, publication, or launch.

## Non-Goals

- No production restore entry point or detached-production fleet capability.
- No provider-side lifecycle fencing generation or new PostgreSQL schema.
- No second-publication takeover protocol for an ambiguous fresh capture.
- No concrete Podman, Docker, filesystem-image, NFS, or cross-host backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-operation-guard.mjs`
- `src/postgres-restore-lifecycle-guard.mjs`
- `src/postgres-restore-activation-recovery-service.mjs`
- `src/postgres-restore-recovery-runner.mjs`
- `src/postgres-restore-recovery-scheduler.mjs`
- Focused operation-guard, lifecycle-guard, recovery-service, recovery-runner,
  scheduler, and writer-detach compatibility tests.
- Real PostgreSQL shared/exclusive exclusion and scheduled empty-sweep coverage.
- Full Node.js suite passed locally. Real PostgreSQL integration remains a CI
  delivery gate because the local database URL was unavailable.
