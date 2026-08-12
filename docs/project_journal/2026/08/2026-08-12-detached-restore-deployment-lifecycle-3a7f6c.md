---
id: 20260812-3a7f6c
title: Detached Restore Deployment Lifecycle
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/postgres-detached-restore-deployment-lifecycle
pr:
supersedes: []
superseded_by:
---

# Detached Restore Deployment Lifecycle

## Summary

- Added one deployment-owned controller over the production-neutral detached-
  restore runtime.
- Bound schema migration, the initial complete recovery sweep, foreground
  admission, and shutdown drain without enabling the production restore
  adapter.
- Kept all four PostgreSQL pools borrowed and left their final close with the
  caller after the controller's stop barrier settles.

## Current State

- The low-level runtime now exposes a narrow `bootstrap.migrate()` facet bound
  to the same internal `PostgresSerializableStore` used by the authority,
  stable-plan registry, and recovery cursors.
- `createPostgresDetachedRestoreRuntimeController({ runtime })` accepts only an
  authentic assembled runtime. `start()` is single-flight: it applies the
  ordered migration chain, starts the bounded recovery scheduler, coalesces
  with its immediate first pass, and opens admission only after an exact
  `completed` receipt reports a full `sweep-complete` result.
- Migration failure, a busy or uncertain initial pass, malformed recovery
  evidence, or scheduler failure leaves that controller terminal and closed.
  A concurrent `stop()` during startup prevents admission from reopening.
- The controller exposes only gated foreground, stable-plan provisioning, and
  same-launcher writer-start capabilities plus `start()` and `stop()`. Every
  admitted asynchronous call is retained until settlement. `stop()` closes
  new admission first, asks the scheduler to stop immediately, and then drains
  the scheduler and all already-admitted calls.
- Each assembled runtime has exactly one controller owner for its lifetime.
  Direct and ordinary Promise-descendant attempts by an admitted call to invoke
  the same controller's `stop()` fail closed. That context check is defensive,
  not an authorization boundary across arbitrary `AsyncResource` replacement:
  only the external deployment owner may hold `stop`, and injected collaborators
  must not invoke it or return a Promise that depends on it.
- The controller never calls `pool.end()`. Deployment closes the four borrowed
  pools only after its stop completion settles. The raw runtime is a low-level
  assembly capability and must not be distributed as a serving ingress beside
  the controller.
- The capture-only backend and its fixed unavailable `runRestore()` authority
  remain unchanged. No final public restore-capable backend exists yet.

## Non-Goals

- No physical provider or image adapter, OCI/container launcher, filesystem-
  image backend, DSN/environment parser, TLS policy, or pool factory is added.
- No operational lease-budget policy is selected for long capture or
  activation windows.
- No complete assembled restart/acknowledgement-loss/ambiguous-outcome matrix
  or final public backend is added, and production restore remains fail-closed.
- No pool ownership transfer, autonomous restore saga, schema change, or new
  recovery lane is introduced.

## Next Steps

- Bind the remaining deployment-owned provider/image and PostgreSQL
  connection/bootstrap configuration plus an explicit operational lease
  budget.
- Run the complete assembled restart and ambiguous-outcome matrix through the
  controlled ingress.
- Construct the final public restore-capable backend only after those gates
  preserve the no-second-writer boundary, then replace the fixed restore stub.

## Evidence

- `src/postgres-detached-restore-runtime-composition.mjs`
- `src/postgres-detached-restore-runtime-controller.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `test/postgres-detached-restore-runtime-controller.test.mjs`
- `integration/postgres-session-authority.mjs`
