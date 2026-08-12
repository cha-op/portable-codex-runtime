---
id: 20260701-6f13a8
title: Portable Runtime Delivery Plan
status: active
created: 2026-07-01
updated: 2026-08-12
branch:
pr:
supersedes: []
superseded_by:
---

# Portable Runtime Delivery Plan

## Summary

- The runtime will be delivered as a sequence of independently reviewed and
  squash-merged pull requests.
- Pull requests remain serial; source research, test design, and focused review
  may run in parallel within the active pull request.

## Current State

- The external `chatgptAuthTokens` consumer boundary is already proven.
- Auth refresh authority, interrupted-turn recovery, session storage contracts,
  the auth broker MVP, snapshot and restore core, stopped-tree primitives,
  durable filesystem operation journal, stopped-directory publication,
  stopped-writer capability, backend composition, and committed capture
  reconciliation, pinned-executable resume evidence, and offline rollout-tail
  repair before writable recovery are complete through PR #13.
- The PostgreSQL serializable authority foundation, initial durable schema,
  real concurrency coverage, and bounded OCI/Docker runnable-image reservation
  are complete in PR #14 without claiming session lifecycle or container
  launch.
- Canonical session registration and strict readback are complete as the first
  production-authority slice without allocating a lease or authorizing a
  writer.
- The serial authority, durable stop/capture, detached activation, prepared
  launch, foreground composition, bounded recovery scheduler, and production-
  neutral runtime assembly foundations are complete. The assembled runtime now
  exposes the same internal launcher's narrow writer-start ingress, preserving
  the process-local opaque handle required by later stop/capture. The
  PostgreSQL durable stable-plan registry now provides separately gated
  immutable provisioning and a read-only foreground resolver. A deployment-
  owned controller now binds migration-before-serving, an initial complete
  recovery sweep, restore admission, and shutdown drain. A concrete PostgreSQL
  deployment now accepts explicit connection/bootstrap policy, constructs and
  owns the four private role pools, performs a point-in-time same-primary
  topology check, and closes every pool only after controller drain. It now
  also owns the exact image-plan provider configuration and private binding
  from an authentic plan's `imagePlanId` through exact OCI bytes and trusted
  inspection to an opaque reservation shared by the controller-gated facet,
  foreground preparation, and launcher revalidation. The physical-collaborator
  settlement foundation now gives those two provider methods separate result
  deadlines and post-deadline grace periods, fresh opaque invocation identities
  and authentic abort signals, and a deployment-private fatal breach route
  without claiming physical quiet or retry authority. Restore activation now
  also performs claim, read-only reconciliation, optional first attachment,
  and finalization inside one coordinator-owned per-operation guard. No grant
  crosses the component boundary, and retained, ambiguous, or copied caller
  state cannot reconstruct a second physical dispatch.
  The production adapter remains fixed fail-closed.
- The complete dependency order and delivery invariants are recorded in
  `docs/architecture/runtime-delivery-plan.md`.

## Next Steps

- Extend method-specific physical-collaborator settlement to mutating
  supervisor, storage-lifecycle, and publication calls, then admit the
  operational lease budget across the complete critical path.
- Validate the complete assembled restart, ambiguous-outcome, deadline, and
  grace-breach matrix, then construct the final public restore-capable backend
  and enable the production adapter only if the no-second-writer boundary
  remains closed.

## Evidence

- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/stopped-directory-publication.md`
- `docs/project_journal/2026/08/2026-08-12-physical-collaborator-settlement-a3f9c2.md`
- `docs/project_journal/2026/08/2026-08-12-restore-activation-reconciliation-b6d4e1.md`
- `docs/project_journal/2026/08/2026-08-12-detached-restore-image-plan-binding-e7b3c9.md`
- `docs/project_journal/2026/08/2026-08-12-postgres-detached-restore-deployment-7d4a91.md`
- `docs/project_journal/2026/08/2026-08-12-detached-restore-deployment-lifecycle-3a7f6c.md`
- `docs/project_journal/2026/08/2026-08-11-detached-restore-stable-plan-registry-8e4c21.md`
- `docs/project_journal/2026/07/2026-07-15-pinned-executable-resume-tail-repair-9d813d.md`
- `docs/project_journal/2026/07/2026-07-29-canonical-session-registry-4e8a2d.md`
- `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`
