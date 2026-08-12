---
id: 20260812-c2e7b4
title: Operational Lease Budget
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/operational-lease-budget
pr:
supersedes: []
superseded_by:
---

# Operational Lease Budget

## Summary

- Added a deployment-owned operational lease policy that derives the minimum
  accepted stable-plan lease from the complete bounded physical graph without
  changing the persisted plan or authority schemas.
- Enforced the same exact lease at stable-plan provisioning and every durable
  resolution, before a foreground invocation can obtain physical authority.

## Current State

- The renewal-to-generation-claim and activation-to-launch-claim intervals are
  separate PostgreSQL-clock windows. Each adds the physical result deadline
  and settlement grace for every sequential boundary it can traverse. The
  writer window counts both fresh checkpoint publication and its possible
  same-call committed-verification fallback. The activation window uses the
  retained-prepared continuation, which combines the fresh attachment dispatch
  with a newly resolved image reservation and launch-time reinspection and,
  because every term is positive, strictly bounds the shorter activation
  branches. Each window then adds the deployment's explicit aggregate database-
  request allowance and positive safety margin. The admitted minimum is the
  maximum of those two windows.
- The configured lease duration must be a safe positive integer no greater
  than 24 hours, meet the derived minimum, and exactly match the lease already
  hashed into every provisioned or rehydrated stable plan. Checked arithmetic
  rejects overflow and an unrepresentable critical path during deployment
  construction, before any settlement coordinator or PostgreSQL pool exists.
- `databaseRequestMilliseconds` is an operator-declared aggregate bound for
  database work inside either clock window. It is not inferred from one
  `query_timeout`, does not replace PostgreSQL's claim-time clock checks, and
  does not turn a latency SLO into dispatch authority. Underestimation can
  still make progress fail closed when the database observes lease expiry.
- The fresh activation claim repeats exact-duration admission before its guard
  and database claim. Retained activation and launch-attempt recovery remain
  read-only/cancellation reconciliation of an earlier grant; they neither
  compare historical rows with the current deployment policy nor regain a
  physical dispatch grant.
- Deadline and grace budgeting does not convert grace into a late-success
  window. A deadline permanently closes result acceptance; grace only drains
  the original Promise or reaches fatal no-settlement. Expiry, abort, or fatal
  shutdown never permits a second physical dispatch or proves quiescence.

## Non-Goals

- This slice does not select production latency values for a concrete storage,
  supervisor, image, network, or PostgreSQL deployment.
- It does not add a schema, renew a lease mid-operation, create retry authority,
  or change the existing database-clock claim and fencing rules.
- It does not claim the assembled restart, acknowledgement-loss, deadline,
  grace-breach, late-settlement, and ambiguity matrix is complete, and it does
  not enable the fail-closed public `runRestore()` route.

## Next Steps

1. Run the assembled restart, acknowledgement-loss, deadline, grace-breach,
   late-settlement, and ambiguous-outcome matrix without a second physical
   dispatch.
2. Construct the immutable final public restore-capable backend, then replace
   the fixed `runRestore()` stub only after that matrix passes.

## Evidence

- `src/postgres-detached-restore-operational-lease-budget.mjs`
- `src/postgres-detached-restore-stable-plan-registry.mjs`
- `src/postgres-detached-restore-runtime-composition.mjs`
- `src/postgres-detached-restore-deployment.mjs`
- `src/postgres-restore-activation-recovery-coordinator.mjs`
- `test/postgres-detached-restore-operational-lease-budget.test.mjs`
- `test/postgres-detached-restore-stable-plan-registry.test.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `test/postgres-restore-activation-recovery-coordinator.test.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
- `integration/postgres-session-authority.mjs`
