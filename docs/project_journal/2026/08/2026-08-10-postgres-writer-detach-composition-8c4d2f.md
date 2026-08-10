---
id: 20260810-8c4d2f
title: PostgreSQL Writer Detach Composition
status: completed
created: 2026-08-10
updated: 2026-08-10
branch: wip/postgres-writer-detach-composition
pr:
supersedes: []
superseded_by:
---

# PostgreSQL Writer Detach Composition

## Summary

The PostgreSQL session authority already owns durable writer-release and
force-fence transitions, while storage backends expose the corresponding
physical mutations. This workstream composes those seams behind one
provider-neutral facade. One guarded invocation reserves the exact operation,
claims provider dispatch, validates the matching physical proof, and finalizes
the durable terminal state without running a provider inside a database
transaction.

## Protected Property

The facade may invoke a provider only after the exact typed dispatch call
returns a definite `dispatchGranted: true` while the per-operation PostgreSQL
advisory guard is held. A replay that observes `starting` or `uncertain` cannot
turn that retained state into a second provider call. It instead records an
uncertain transition when needed and finalizes the exact operation as
`BLOCKED`. A valid provider proof is retained within the live invocation: if
database finalization loses its acknowledgement, the facade may reconcile and
replay only that exact finalizer, never the provider mutation.

The backend identity and complete capability tuple must match the canonical
session snapshot before the first authority write. Provider request envelopes
come only from the typed authority claim and provider results must echo those
envelopes through the storage contract validators. Database epoch allocation,
lease expiry, a generic detach result, and caller assertions are not physical
fence proof.

## Scope

- Compose exact writer release and force-fence operations with the existing
  PostgreSQL authority, per-operation advisory guard, and storage backend v1.
- Invoke `detachAttachment()` or `forceFence()` only from one definite typed
  dispatch grant and validate the complete matching provider proof.
- Convert provider throws, rejected or unsafe asynchronous results, malformed
  proofs, and unresolvable pre-proof state into durable `BLOCKED` outcomes.
- Record manual fencing as `fence-unavailable` without invoking the provider.
- Reconcile reserve, dispatch, uncertainty, blocked-finalization, and
  successful-finalization acknowledgement loss without duplicating a physical
  mutation.
- Add focused unit coverage and real-PostgreSQL integration coverage without
  changing the authority schema.

## Acceptance Criteria

- Release success preserves the old writer epoch, reaches `DETACHED`, and
  clears the canonical lease and attachment only after the exact detach proof.
- Force-fence success advances the writer epoch at the definite dispatch
  commit and reaches `DETACHED` only after the dedicated exact fence proof.
- Exact terminal replay returns the durable operation, reservation, and
  session without another provider call or state write.
- `prepared` state may continue to its first dispatch. Retained `starting` or
  `uncertain` state never authorizes provider replay and becomes durably
  `BLOCKED`.
- Provider ambiguity becomes `provider-outcome-unresolved`; a manual backend
  becomes `fence-unavailable`. Neither path fabricates successful proof.
- A valid proof followed by finalization acknowledgement loss reconciles or
  replays only the same database finalizer.
- Backend, session, operation, request, target, lease, epoch, and proof drift
  fail closed before a mismatched physical or durable transition.
- Production restore remains fail-closed and no scheduler invokes the facade.

## Implementation

- `createPostgresWriterDetachComposition()` captures the authority and backend
  collaborators plus one branded `PostgresOperationGuard`, then returns a
  branded frozen facade with explicit release and force-fence methods.
- Authority receipts are boundedly cloned and accepted only after the
  canonical operation, reservation, active or terminal session pointer, typed
  result, and provider proof pass
  `assertSessionOperationTransitionProof()` as one relation.
- Each method builds one canonical typed operation from the caller's complete
  pre-reserve session snapshot, operation ID, and attachment target. That same
  snapshot and request remain the base for every reserve, claim, reconciliation,
  uncertainty, and finalization call.
- The operation guard spans durable admission, provider execution, and
  terminal finalization. The provider receives only the authority-generated
  mutation or fence envelope.
- A committed readback after finalizer acknowledgement loss is accepted only
  when its persisted provider result validates against the original claim
  envelope and carries the exact proof ID returned by this invocation.
- Stable public completion omits replay-sensitive flags such as `acquired`,
  `dispatchGranted`, and `finalized`; it returns only the durable operation,
  reservation, and session.

## Recovery Limit

Storage backend contract v1 has no provider-side reconciliation operation by
durable operation ID. After a crash or dispatch-commit acknowledgement loss,
observing `starting` proves that dispatch was durably claimed but does not prove
whether the physical call ran. The facade therefore does not retry that call;
it fails closed to `BLOCKED`. Automatic provider-outcome recovery requires a
future, separately versioned backend reconciliation extension plus an authority
read path that can reconstruct the complete typed operation and retained target.

## Non-Goals

- No automatic release-to-force-fence fallback.
- No provider-specific filesystem, NFS, block-volume, or image implementation.
- No provider reconciliation extension or typed detach recovery enumerator.
- No migration, new table, recovery-cursor change, or launch-state change.
- No detached-production fleet capability, checkpoint-adapter scheduling, or
  `runRestore()` enablement.
- No differential compression, retention, encryption, cross-host transport,
  auth change, periodic snapshot scheduler, or Git Summary.
