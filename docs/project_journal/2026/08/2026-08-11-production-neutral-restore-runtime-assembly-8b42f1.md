---
id: 20260811-8b42f1
title: Production-Neutral Restore Runtime Assembly
status: completed
created: 2026-08-11
updated: 2026-08-11
branch: wip/postgres-detached-restore-runtime-composition
pr:
supersedes: []
superseded_by:
---

# Production-Neutral Restore Runtime Assembly

## Summary

Completed the production-neutral runtime assembly foundation for detached
restore. One strict factory now constructs the capture-only checkpoint
backend, standalone foreground restore facade, and idle bounded-recovery
scheduler from one internally consistent authority graph and four distinct
caller-owned pool objects. The factory proves that those objects are pairwise
distinct, not that their underlying PostgreSQL routes are independent.
Construction performs no migration, database query, scheduler start, physical
provider action, or pool shutdown.

The production checkpoint adapter remains deliberately unchanged. Its
`runRestore()` path is still fixed fail-closed, and the assembled foreground
facade is not injected into the capture-only backend.

## Current State

- `createPostgresDetachedRestoreRuntimeComposition()` accepts four borrowed
  pools plus the lifecycle backend, publication and image/stopped-writer
  coordinators, supervisor, stable-plan and storage resolvers, rollout
  decisions, and recovery policy. It constructs the store, authority, three
  operation guards, lifecycle guard, logical launcher, checkpoint mutation
  authority, capture-only stopped-directory backend, durable stop/capture
  facade, writer detach facade, activation coordinator, detached-restore
  foreground facade, recovery service, cursor store, runner, and scheduler in
  one closed object graph.
- The returned frozen capability surface is exactly `{ backend, foreground,
  scheduler }`. Internal authorities, guards, cursors, and runners are not
  exposed or replaceable after construction.
- Every pair of supplied pool objects must be distinct before any pool method
  is read or any component is constructed. Descriptor-only preflight also
  rejects Proxy or accessor-backed pool and storage-backend fields before an
  external getter or trap can run.
- The pool check proves only object identity. Deployment must still prove that
  all four pools use the same authoritative PostgreSQL primary, retain the
  required session affinity, have adequate independent capacity, and avoid
  transaction or statement pooling.
- The checkpoint backend remains capture-only and retains the existing fixed
  unavailable restore authority. The separately returned foreground facade
  retains restore context contract version 3, but no production adapter calls
  it yet.
- The scheduler is returned idle. The caller owns schema migration, scheduler
  start and stop, foreground admission and drain, and pool shutdown. Runtime
  construction and scheduler idle-stop do not close caller-owned pools.
- Compatibility decisions remain explicit inputs. The assembly does not
  silently enable writer-stop V3, activation V2, generation-predecessor
  compatibility, restore-generation V2, or invocation-time detached-production
  fleet admission.
- Existing coordinator classes that expose no public authentic-brand
  predicate retain their existing constructor contracts. Runtime assembly
  does not claim that `instanceof` alone proves their private internal state;
  an unusable lookalike fails closed when its capability is exercised.

## Validation Boundary

- Unit coverage proves construction is zero-I/O, all six pairwise pool aliases
  fail before connection acquisition, the capture-only restore path remains
  unavailable, the standalone foreground contract remains version 3, and
  caller lifecycle ownership is unchanged.
- Hostile-shape coverage proves that extra fields, accessors, Proxy prototype
  chains, invalid downstream options, and externally constructed domain-error
  lookalikes cannot trigger external behavior or forge the runtime brand.
- Real PostgreSQL coverage uses four pairwise-distinct Pool instances and
  caller-owned migration/start/stop/end operations to exercise the assembled
  empty recovery sweep, durable cursor updates, shared-foreground/exclusive-
  recovery exclusion, scheduler shutdown, and default-deny foreground gate.
- Existing component suites remain the authority for acknowledgement-loss,
  prepared-generation continuation, no-second-publication, provider
  ambiguity, and no-relaunch behavior. This assembly slice does not duplicate
  or weaken those contracts.

## Next Steps

1. Provide deployment-owned durable stable-plan resolution, physical storage
   and image/provider bindings, PostgreSQL bootstrap and migration ownership,
   fleet compatibility decisions, lease budgets, and explicit runtime
   admission/drain policy.
2. Construct the final public restore-capable backend only after the private
   capture backend and standalone foreground facade exist; do not introduce a
   mutable placeholder or post-construction method swap.
3. Run the complete assembled restart and ambiguous-outcome matrix with the
   deployment bindings, then replace the production checkpoint adapter's
   fixed `runRestore()` stub only when no second writer, capture, publication,
   activation, image reservation, or physical launch is possible.

## Non-Goals

- No production `runRestore()` enablement or public V3 checkpoint backend.
- No automatic migration, scheduler start, shutdown manager, or pool close.
- No stable-plan database schema, autonomous top-level restore saga, or new
  recovery lane.
- No environment-variable/DSN parser, deployment daemon, or concrete
  Podman/Docker/ext4/filesystem-image/NFS/cross-host backend.
- No change to existing acknowledgement-loss or no-relaunch authorities.

## Evidence

- `src/postgres-detached-restore-runtime-composition.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `integration/postgres-session-authority.mjs`
- `src/postgres-detached-restore-foreground-composition.mjs`
- `src/postgres-restore-recovery-scheduler.mjs`
