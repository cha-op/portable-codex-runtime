---
id: 20260730-7b3e92
title: Writer Lease and Attachment Acquisition
status: completed
created: 2026-07-30
updated: 2026-07-30
branch: wip/writer-lease-attachment
pr:
supersedes: []
superseded_by:
---

# Writer Lease and Attachment Acquisition

## Summary

- Completed the typed writer lease, attachment acquisition, exact
  finalization, and renewal slice on the existing PostgreSQL authority schema.
- Kept provider execution outside database transactions and kept physical
  fencing, release, force-fence, launcher admission, and physical-backend
  authority outside this slice.

## Current State

- The slice requires no DDL. It stores the lease, lifecycle, attachment, active
  pointer, and terminal anchor in the existing canonical JSONB document and
  reuses the existing operation and reservation rows.
- Generic `reserveOperation()` first claims the exact
  `writer-attachment-acquire-v1` request. The typed
  `claimWriterAttachmentDispatch()` transaction then locks the exact session,
  operation, and reservation state, reads PostgreSQL `clock_timestamp()`,
  allocates a lease duration bounded to 1 through 86,400,000 milliseconds,
  derives deterministic lease and attachment IDs from the operation ID,
  advances the decimal-string uint64 writer epoch, and commits
  `prepared -> starting` with lifecycle `ATTACHING`. Before granting dispatch,
  it also reserves enough PostgreSQL bigint revision capacity to record
  `starting -> uncertain -> committed`.
- The dispatch receipt carries the exact attach mutation request. The caller
  invokes the provider only after that transaction commits; the authority
  never runs a provider callback inside the transaction.
- `finalizeWriterAttachment()` binds the exact successful mutation result to
  the exact attachment proof and can atomically finalize either `starting` or
  `uncertain` to `ATTACHED`. The same transaction commits the operation,
  releases the reservation, clears `activeOperation`, persists the lease and
  physical attachment evidence, and writes `lastOperation`.
- Exact physical evidence remains durable even when the provider finishes
  after lease expiry. Expiry closes subsequent admission; it does not erase
  attachment evidence, change the physical state, or prove a fence.
- `renewWriterLease()` is one provider-free `SERIALIZABLE` transaction. It
  reads database clock time, rejects equality with `expiresAt` as already
  expired, preserves the complete lease and attachment tuple, changes only
  `expiresAt`, and writes a revision-zero committed operation, released
  reservation, and terminal anchor. The globally unique operation ID makes an
  exact replay return the original result without extending the lease again;
  an expired lease cannot be revived.
- Dispatch, finalization, and renewal retain exact replay and commit-outcome
  reconciliation semantics. Epoch exhaustion, mismatched evidence, stale
  snapshots, and conflicting operation-ID reuse fail closed.

## Next Steps

- Implement exact-owner release and force-fence reconciliation, including the
  `FENCING` and `BLOCKED` lifecycle outcomes. Require verified physical fence
  evidence before treating a former writer as excluded; neither lease expiry
  nor database epoch allocation is that evidence.

## Evidence

- `src/postgres-session-authority.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
- `docs/architecture/runtime-delivery-plan.md`
