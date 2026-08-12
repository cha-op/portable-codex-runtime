---
id: 20260811-8e4c21
title: Detached Restore Stable-Plan Registry
status: completed
created: 2026-08-11
updated: 2026-08-12
branch: wip/postgres-detached-restore-stable-plan-registry
pr:
supersedes: []
superseded_by:
---

# Detached Restore Stable-Plan Registry

## Summary

- Added migration 7 and a PostgreSQL-backed immutable stable-plan registry.
- Split separately gated provisioning from read-only foreground resolution.
- Wired the registry into the production-neutral runtime without enabling the
  production checkpoint adapter's restore route.

## Current State

- `provisionStablePlan({admission, plan})` validates the shared clean-checkpoint
  admission and authentic plan capability, requires its dedicated fleet
  confirmation, and atomically inserts or exactly replays one durable binding.
- The restore operation ID is permanently reserved in the shared authority
  namespace. Crossed session, request, or plan identity fails closed, and a
  lost commit acknowledgement succeeds only after exact durable readback and
  otherwise remains uncertain for exact retry.
- Neither the plan nor its materialized operation can be deleted while the
  permanent preclaim remains; complete authority teardown removes all three in
  one transaction. The preclaim's identity and binding are immutable, and its
  sole materialization transition must prove the exact version 1 operation by
  transaction commit. Database constraints reject missing, null, or
  type-coerced durable request, digest, and plan-input fields instead of
  accepting SQL `UNKNOWN`. Generation dispatch revalidates the full rehydrated
  plan, permanent plan digest, generation ID, and destination-isolation proof
  ID before publication can be authorized.
- `resolveStablePlan({admission, expectedSession})` performs read-only durable
  verification and rehydrates the plan from canonical inputs. It never creates
  or repairs state.
- The runtime constructs the registry from its existing internal store. Its
  public frozen null-prototype `stablePlanProvisioning` facet exposes only the
  receiver-preserving provisioning wrapper; the receiver-preserving resolver
  is private to the foreground composition.
- Foreground and registry now share one restore-admission validator. Invalid or
  crossed admission is rejected before lifecycle, authority, gate, or provider
  effects.
- The runtime remains production-neutral and the capture backend retains its
  fixed unavailable `runRestore()` implementation.

## Non-Goals

- No physical provider/image, PostgreSQL bootstrap, lease-budget,
  admission/drain, or final public-backend binding is added here.
- No autonomous restore saga or resolver-side provisioning/repair is added.
- Production restore is not enabled.

## Next Steps

- Supply the remaining deployment-owned bindings.
- Run the complete assembled restart and ambiguous-outcome matrix.
- Construct the final public restore-capable backend only after those gates
  preserve the no-second-writer boundary.

## Evidence

- `migrations/authority/007-detached-restore-stable-plans.sql`
- `src/postgres-detached-restore-stable-plan-registry.mjs`
- `src/postgres-session-authority.mjs`
- `src/postgres-detached-restore-runtime-composition.mjs`
- `test/postgres-detached-restore-stable-plan-registry.test.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/postgres-serializable-store.test.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `integration/postgres-session-authority.mjs`
