---
id: 20260812-a3f9c2
title: Physical Collaborator Settlement
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/physical-collaborator-settlement
pr:
supersedes: []
superseded_by:
---

# Physical Collaborator Settlement

## Summary

- Added a reusable two-stage settlement boundary for an asynchronous physical
  collaborator: one result deadline followed by one bounded settlement grace.
- Applied the foundation to deployment-owned image-plan resolution and trusted
  Codex inspection as its first consumer, without opening production restore.

## Current State

- `createPhysicalCollaboratorSettlement()` binds exact positive
  `deadlineMilliseconds` and `settlementGraceMilliseconds` values, each at most
  86,400,000, plus one owner-only fatal callback. A fresh invocation receives a
  fresh authentic abort signal.
- The deadline permanently closes result acceptance and requests cooperative
  abort. The grace period observes only the original Promise; a late fulfilment
  remains uncertain rather than becoming success. Timer callbacks and provider
  reactions recheck a captured monotonic clock, so an event-loop stall cannot
  extend deadline or grace.
- A Promise that does not settle through grace invokes the owner-only fatal
  callback at most once for that invocation. This is a sticky deployment
  failure signal, not proof that an external effect is quiet; later settlement
  cannot turn that deployment into a clean stopped result.
- Image-plan provider contract version 2 carries a fresh frozen null-prototype
  zero-field opaque invocation identity and authentic signal as own enumerable
  fields. Resolver input has exact keys `imagePlanId`, `imagePlanProviderId`,
  `invocation`, `sessionManifest`, and `signal`; inspector input has exact keys
  `imagePlanId`, `imagePlanProviderId`, `inspection`, `invocation`, and
  `signal`. The two methods use distinct deployment-selected policies under
  `runtime.launch.imagePlanProviderSettlement`.
- Deployment privately maps a no-settlement breach to its existing fatal
  shutdown path. The provider cannot select its policy, obtain the fatal hook,
  or capture the deployment's lifecycle `stop` capability.
- The real-PostgreSQL deployment representative proves successful policy
  wiring, fresh opaque invocation identities and non-aborted signals, exact
  provider inputs, image reservation and revalidation, and clean pool shutdown
  without adding a timer-sensitive breach case to the database integration
  suite.

## Non-Goals

- An abort signal is cooperative. It does not prove cancellation or quiescence
  of a callback, network request, child process, filesystem mutation, or other
  physical side effect, and a timer cannot preempt a callback that blocks the
  event loop.
- Deadline, grace expiry, or fatal shutdown does not authorize retry, takeover,
  or a second provider, launch, storage, or publication dispatch.
- The provider is a trusted deployment collaborator, not a hostile-code
  sandbox. An unreplaceable throwing own Promise `constructor` makes standard
  `then`/`await` observation impossible; the boundary fails closed and starts
  fatal shutdown but cannot suppress that provider-created unhandled rejection
  without weakening process-global isolation. Hostile providers require a
  process boundary and an ordinary native-Promise adapter.
- These settlement policies are not the operational database-lease budget and
  are not writer-fencing evidence.
- This slice does not yet wrap mutating supervisor, storage-lifecycle, or
  publication collaborators. It selects no production registry, process, or
  container timeout defaults.
- No registry fetch, publisher/signature verification, concrete container
  launch, physical storage backend, schema, migration, or recovery lane is
  added. The production `runRestore()` route remains fixed fail-closed.

## Next Steps

1. Apply method-specific settlement to the mutating supervisor, storage-
   lifecycle, and publication collaborators without granting duplicate
   dispatch after ambiguity.
2. Admit an operational lease budget across the full stop, capture,
   publication, detach, activation, and launch critical path.
3. Run the assembled restart, acknowledgement-loss, deadline, grace-breach,
   and ambiguous-outcome matrix.
4. Construct the final public restore-capable backend and enable `runRestore()`
   only if that matrix preserves the no-second-writer boundary.

## Evidence

- `src/physical-collaborator-settlement.mjs`
- `src/postgres-detached-restore-image-plan-binding.mjs`
- `src/postgres-detached-restore-deployment.mjs`
- `test/physical-collaborator-settlement.test.mjs`
- `test/postgres-detached-restore-image-plan-binding.test.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
