---
id: 20260731-e4b9c7
title: Writer Release and Force-Fence Reconciliation
status: completed
created: 2026-07-31
updated: 2026-07-31
branch: wip/release-force-fence
pr:
supersedes: []
superseded_by:
---

# Writer Release and Force-Fence Reconciliation

## Summary

- Completed the PostgreSQL writer release, force-fence, `FENCING`, and
  `BLOCKED` authority slice on the existing schema.
- Kept every provider outside database transactions and kept physical backend
  fencing, production checkpoint catalogue authority, and launcher admission
  outside this slice.

## Current State

- The slice adds `writer-release-v1` and `writer-force-fence-v1` without DDL.
  Both use generic `reserveOperation()`, typed dispatch, provider execution
  outside the transaction, and typed finalization.
- `claimWriterReleaseDispatch()` starts only from the exact `ATTACHED` snapshot
  and attachment target. It enters `RELEASING` while preserving the complete
  lease tuple and writer epoch. An exact-owner detach may finish after lease
  expiry only for that unchanged tuple, storage identity, and target; release
  never advances the epoch.
- `finalizeWriterRelease()` accepts only the exact matching successful detach
  result from `starting` or `uncertain`, then atomically clears the attachment
  and lease, enters `DETACHED`, retires the operation, releases the
  reservation, and writes the terminal anchor. Exact replay performs no write.
- `claimWriterForceFenceDispatch()` starts only from an exact `ATTACHED` or
  `BLOCKED` snapshot. Its definite dispatch commit advances the canonical
  decimal-string uint64 epoch once and enters `FENCING`; replay, restart, or
  commit uncertainty cannot dispatch or advance it again.
- The dedicated force-fence request binds the new epoch, exact revoked lease
  tuple, attachment target, storage identity, and operation ID. Only the exact
  independent result with `status: "fenced"` and a provider proof may finalize
  `FENCING -> DETACHED`. Database expiry, an advanced epoch, a generic detach
  result, or a caller assertion is not physical fence evidence.
- Ambiguous acquire, release, or force-fence outcomes first become
  `uncertain`, then use `finalizeWriterOperationBlocked()` to retire the exact
  typed operation into `BLOCKED`. The terminal result preserves the lease,
  known attachment, force-fence target, and current epoch. An unavailable
  force fence likewise enters `BLOCKED` and retains the epoch already advanced
  by dispatch.
- `BLOCKED -> FENCING` is an explicit recovery edge. It requires a new exact
  force-fence reservation and definite typed dispatch commit, which advances
  the epoch again before a provider is called.
- A backend declaring `fencing: "manual"` cannot successfully complete the
  automatic force-fence proof path. The current database epoch and expiry are
  logical admission state only, never a physical host fence.
- Unit and real-PostgreSQL coverage exercise exact replay, target and tuple
  conflicts, expired exact-owner release, retained ambiguity, manual-backend
  rejection, epoch exhaustion and retention, and explicit blocked recovery.

## Next Steps

- Implement the production checkpoint mutation authority and catalogue,
  binding durable capture attempts to exact checkpoint-catalogue
  finalization.

## Evidence

- `src/postgres-session-authority.mjs`
- `src/session-storage-contracts.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/session-storage-contracts.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
- `docs/architecture/session-storage-contracts.md`
- `docs/architecture/runtime-delivery-plan.md`
