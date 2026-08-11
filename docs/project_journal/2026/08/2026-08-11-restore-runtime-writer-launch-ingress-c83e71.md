---
id: 20260811-c83e71
title: Restore Runtime Writer-Launch Ingress
status: completed
created: 2026-08-11
updated: 2026-08-11
branch: wip/postgres-detached-restore-writer-launch-ingress
pr:
supersedes: []
superseded_by:
---

# Restore Runtime Writer-Launch Ingress

## Summary

Closed the runtime object-graph reachability gap between ordinary writer start
and detached restore. The production-neutral runtime now exposes a narrow
writer-launch facet bound to the exact process-local logical launcher already
used by the capture backend, foreground restore composition, and recovery
graph.

The protected property is opaque writer-handle identity: only the launcher
that performed and registered a successful same-process start owns the handle
later required by V3 stop-to-prepared-capture. A durable committed launch row,
serialized image measurement, another runtime, or another launcher is not a
substitute.

## Current State

- The runtime's exact frozen surface is `{ backend, foreground, scheduler,
  writerLaunch }`.
- `writerLaunch` is an exact frozen null-prototype facet containing only
  `runLaunch()` and `reconcileLaunchAttempt()`.
- Both methods are frozen, receiver-preserving wrappers over the same internal
  launcher. Caller-controlled `this` cannot replace that receiver.
- Stop, retire, prepared-launch, stopped-writer resolution, and internal local
  handle indexes remain private to the assembled graph.
- Construction still performs no migration, pool access, scheduler start,
  provider mutation, image consumption, supervisor call, or pool close.
- The returned backend remains capture-only. Its restore route and the
  production checkpoint adapter's fixed fail-closed `runRestore()` stub are
  unchanged.

## Validation Boundary

- Unit coverage proves the exact runtime and facet shapes, frozen data-method
  descriptors, per-runtime method identity, hidden launcher capabilities,
  receiver capture, and zero-I/O construction/failure behavior.
- Real PostgreSQL integration uses the exposed facet and the same assembled
  authority graph to establish the current writer before exercising the
  detached-restore foreground path. It proves that the old missing-local-
  handle boundary is no longer the reason the same-runtime path stops and that
  no second physical launch occurs.
- Existing logical-launcher tests remain authoritative for committed rows
  without the original local handle, same-process starting/uncertain
  reconciliation, and no-relaunch behavior.

## Next Steps

1. Add a PostgreSQL durable stable-plan registry. Provisioning must happen
   through a separately fleet-gated immutable insert-or-compare API; the
   foreground resolver must be read-only and validate the complete request and
   plan digest.
2. Add the remaining provider/image/bootstrap, lease-budget, lifecycle, and
   foreground admission/drain bindings.
3. Construct the final public V3 backend and run the assembled restart and
   ambiguous-outcome matrix without changing the production route.
4. Replace the checkpoint adapter's fixed restore stub only after that evidence
   preserves the no-second-writer, publication, activation, reservation, and
   physical-launch boundaries.

## Non-Goals

- No stable-plan schema or provisioning API.
- No public restore-capable backend or production `runRestore()` enablement.
- No injected or fully exposed launcher, handle map, stop, retire, or prepared-
  launch capability.
- No cold-process reconstruction or adoption of an opaque writer handle.
- No automatic migration, scheduler lifecycle, admission/drain manager, pool
  shutdown, or concrete Podman/Docker/filesystem-image deployment binding.

## Evidence

- `src/postgres-detached-restore-runtime-composition.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `integration/postgres-session-authority.mjs`
- `src/postgres-logical-writer-launcher.mjs`
