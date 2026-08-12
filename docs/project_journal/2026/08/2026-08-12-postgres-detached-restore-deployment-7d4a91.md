---
id: 20260812-7d4a91
title: PostgreSQL Detached Restore Deployment
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/postgres-detached-restore-postgres-deployment
pr:
supersedes: []
superseded_by:
---

# PostgreSQL Detached Restore Deployment

## Summary

- Added the concrete PostgreSQL connection/bootstrap and pool-ownership
  boundary above the detached-restore runtime controller.
- Kept production restore fail-closed while making migration, initial recovery,
  gated ingress, shutdown drain, and final pool closure one deployment-owned
  lifecycle.

## Current State

- `createPostgresDetachedRestoreDeployment()` accepts exact explicit host,
  port, database, user, credential, TLS, timeout, application-name, and
  per-role capacity configuration. It accepts no DSN and does not use ambient
  `PG*` connection defaults. In verified-TLS mode, `serverName` must exactly
  equal `host`.
- The deployment privately creates pairwise-distinct authority,
  per-operation, foreground-lifecycle, and recovery-lifecycle `pg.Pool`
  instances and passes them into one internally consistent low-level runtime.
  It exposes only the controller's gated foreground, stable-plan-provisioning,
  writer-launch, start, and stop capabilities.
- Before migration, startup holds one checked-out connection from each role.
  It requires the configured database, PostgreSQL 13 or newer, a writable
  non-recovery server, distinct backend sessions, and one shared advisory-lock
  domain across all four connections. Only then may controller migration and
  the initial complete four-lane recovery sweep open ingress.
- The topology result is a point-in-time startup proof. It does not prove that
  DNS, a connection proxy, failover routing, or later replacement connections
  continue to target the same primary; deployment operations retain that
  responsibility.
- Stop first closes controller ingress and drains the scheduler plus admitted
  calls. It then attempts and awaits closure of recovery-lifecycle,
  foreground-lifecycle, per-operation, and authority pools in that order.
  The lifecycle `stop` facet is an owner-only capability and must not be
  captured or awaited by injected runtime collaborators. Same-context direct
  and ordinary Promise-descendant misuse fails closed; arbitrary
  `AsyncResource` context replacement is outside that defensive check.
  Startup failure uses the same terminal drain-and-close path: `start()`
  rejects with the fixed deployment outcome error, while a clean automatic
  cleanup leaves a memoized fulfilled stopped receipt for later `stop()`
  calls. Cleanup or fatal-shutdown failure instead leaves the memoized stop
  result rejected with that deployment outcome error.

## Non-Goals

- No provider/image binding, container launcher, filesystem-image backend, or
  physical fence is added.
- No operational lease budget or long-running capture deadline is selected.
- No DSN or environment parser, credential rotation service, continuous
  topology monitor, failover manager, or connection-discovery mechanism is
  introduced.
- No final public restore-capable backend or adapter routing is added;
  `runRestore()` remains fixed fail-closed.
- No schema, recovery lane, or autonomous cross-stage saga is added.

## Next Steps

- Bind the physical provider/image and operational lease budget.
- Run the complete assembled restart and ambiguous-outcome matrix through the
  deployment-controlled ingress.
- Construct and enable the final restore-capable backend only after that matrix
  preserves the no-second-writer boundary.

## Evidence

- `src/postgres-detached-restore-deployment.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
