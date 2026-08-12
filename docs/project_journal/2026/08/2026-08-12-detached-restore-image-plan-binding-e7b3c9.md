---
id: 20260812-e7b3c9
title: Detached Restore Image Plan Binding
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/postgres-detached-restore-image-plan-binding
pr:
supersedes: []
superseded_by:
---

# Detached Restore Image Plan Binding

## Summary

- Added the deployment-owned binding from an authentic plan's `imagePlanId`
  through exact OCI bytes and trusted Codex inspection to an opaque
  process-local image reservation.
- Bound foreground preparation and logical-launcher revalidation to the same
  private authority without opening the production restore adapter.

## Current State

- One exact provider configuration names the image-plan provider and its
  resolver and trusted inspector. Deployment constructs the binding privately;
  the low-level runtime accepts the authentic binding directly for controlled
  assembly and tests. Both callbacks settle exact frozen null-prototype records,
  preventing inherited-`then` assimilation before binding validation.
- Preparation resolves an authentic plan's `imagePlanId` to exact OCI
  platform-manifest and config bytes, verifies their bounded runnable-image
  projection against the session manifest and trusted inspection, and returns
  an opaque zero-field reservation. The gated deployment facet does not expose
  those bytes, the provider, the coordinator, or the raw binding.
- The same binding is passed to foreground preparation and the logical
  launcher, which revalidates the opaque reservation against the original
  bytes and inspection before the durable launch boundary can progress.
- The real-PostgreSQL representative starts the deployment, provisions and
  reads back an authentic stable plan, then exercises image resolution,
  inspection, opaque reservation, and same-binding revalidation. It
  intentionally stops at a malformed generation request before durable
  `starting` or supervisor dispatch; physical restore-provider, publication,
  and concrete-supervisor effects remain zero.
- The production checkpoint adapter remains capture-only and its fixed
  `runRestore()` route remains fail-closed.

## Non-Goals

- No registry fetch, signature or publisher-trust verification, concrete
  runtime-image pin, container launch, or supervisor implementation is added.
- No physical provider or storage backend, filesystem-image execution, lease-
  budget policy, whole-graph restart/ambiguity matrix, or final public restore
  backend is added.
- The opaque reservation is process-local image identity authority, not a
  durable launch capability or physical writer fence.

## Next Steps

- Bound physical-collaborator settlement and deadlines.
- Admit an explicit operational lease budget.
- Run the whole assembled restart and ambiguous-outcome matrix.
- Construct the final public restore-capable backend only after those gates
  preserve the no-second-writer boundary.

## Evidence

- `src/postgres-detached-restore-image-plan-binding.mjs`
- `src/postgres-detached-restore-runtime-composition.mjs`
- `src/postgres-detached-restore-deployment.mjs`
- `src/postgres-logical-writer-launcher.mjs`
- `integration/postgres-session-authority.mjs`
- `test/postgres-detached-restore-image-plan-binding.test.mjs`
- `test/postgres-logical-writer-launcher.test.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `test/postgres-detached-restore-runtime-controller.test.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
