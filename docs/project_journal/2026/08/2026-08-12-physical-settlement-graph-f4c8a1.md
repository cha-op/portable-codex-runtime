---
id: 20260812-f4c8a1
title: Physical Settlement Graph
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/physical-settlement-graph
pr:
supersedes: []
superseded_by:
---

# Physical Settlement Graph

## Summary

- Extended the deployment-owned physical-collaborator settlement foundation
  from image resolution and inspection to the complete currently assembled
  supervisor, storage lifecycle, publication, and restore-destination resolver
  graph.
- Preserved every existing durable dispatch grant, readback, reconciliation,
  and no-second-dispatch rule while keeping transient invocation identities and
  abort signals outside durable requests and results.

## Current State

- `createPostgresDetachedRestorePhysicalBindings()` privately owns seventeen
  method-specific settlement coordinators: three supervisor methods, nine
  storage-lifecycle methods, four publication methods, and one restore-
  destination resolver. Together with the two image-provider coordinators, the
  PostgreSQL deployment owns nineteen bounded physical boundaries.
- The raw supervisor contract uses transient contract version 2 inputs carrying
  a fresh opaque invocation and authentic abort signal. Its adapter exposes the
  existing logical-launcher version 1 facade, including a settlement-backed
  wrapper for each returned physical `stopWriter` capability. The raw callback
  returns exact transient `{ contractVersion: 2, status: "stopped" }`, which
  the adapter maps to the launcher's existing opaque stop sentinel; durable
  launch and stop request/result versions are unchanged.
- Storage lifecycle contract version 1 remains the durable API. Raw lifecycle
  callbacks receive a separate version 1 frozen invocation context, so
  activation request hashes and every other durable storage request remain
  unchanged. Activation preparation and read-only reconciliation remain under
  the coordinator's guarded one-shot grant protocol.
- Publication callbacks and restore-destination resolution are bounded at the
  lowest external Promise. Fresh publication, committed-only verification,
  and resolver paths retain their existing method-specific durable authority;
  a deadline, late result, or no-settlement breach never changes a verifier
  into publication authority or grants a retry.
- Deployment keeps a private registry containing the aggregate seventeen-
  method graph stop plus the two image-provider stops. Shutdown first closes
  admission and synchronously starts those three capabilities; the graph stop
  in turn starts all seventeen method stops before awaiting any one. It then
  drains the controller and every settlement before closing the four PostgreSQL
  pools. Every stop and pool close is attempted; any failure is sticky and
  cannot be reported as a clean stopped deployment.

## Non-Goals

- Abort remains cooperative. It does not prove process, network, filesystem,
  provider, or child-operation quiescence, and grace breach plus fatal shutdown
  is not fencing evidence.
- Settlement never authorizes retry, takeover, a second launch or stop, a
  second storage mutation, or a second fresh publication after an ambiguous
  outcome. Existing durable readback, stopped-only reconciliation, committed-
  only verification, and blocked detach/fence recovery remain authoritative.
- This slice does not choose or admit the operational lease budget. It does not
  claim the assembled restart, acknowledgement-loss, deadline, grace-breach,
  and ambiguity matrix is complete.
- No physical storage provider, registry fetch, publisher/signature trust,
  container runtime, schema, migration, final public backend, or production
  `runRestore()` route is added.

## Next Steps

1. Derive and enforce the operational lease budget from every method-specific
   deadline and grace period across the complete critical path.
2. Run the assembled restart, acknowledgement-loss, deadline, grace-breach,
   late-settlement, and ambiguous-outcome matrix without a second physical
   dispatch.
3. Construct the immutable final public restore-capable backend, then replace
   the fixed fail-closed `runRestore()` stub only after that matrix passes.

## Evidence

- `src/postgres-detached-restore-physical-bindings.mjs`
- `src/postgres-detached-restore-deployment.mjs`
- `src/stopped-directory-backend.mjs`
- `src/postgres-restore-activation-recovery-coordinator.mjs`
- `src/postgres-detached-restore-runtime-composition.mjs`
- `test/postgres-detached-restore-physical-bindings.test.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
- `test/stopped-directory-backend.test.mjs`
- `test/postgres-restore-activation-recovery-coordinator.test.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
