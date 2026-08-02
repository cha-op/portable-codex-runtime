---
id: 20260731-c3a8f2
title: Production Checkpoint Mutation Authority
status: completed
created: 2026-07-31
updated: 2026-07-31
branch: wip/checkpoint-mutation-authority
pr:
supersedes: []
superseded_by:
---

# Production Checkpoint Mutation Authority

## Summary

- Added the production PostgreSQL authority boundary for stopped-writer clean
  capture and exact checkpoint-catalogue finalization.
- Kept the slice capture-only. Restore remains fail-closed until canonical
  destination-generation authority and logical launcher admission are
  composed.

## Current State

- The capture authority reuses the version 1 session-authority schema without
  DDL. Existing operation, reservation, capture-attempt, tombstone, and
  checkpoint-catalogue rows carry the complete durable state.
- One exact session-wide operation and reservation binds the canonical
  `ATTACHED` snapshot, unexpired database-clock lease, attachment, storage,
  checkpoint descriptor, mutation request, stopped-writer correlation, and
  predetermined result.
- A typed definite dispatch atomically enters `starting` and claims the exact
  backend-minted capture-attempt UUID before publication can run. Operation and
  attempt identities are global and non-reusable.
- A per-operation PostgreSQL session advisory guard spans claim activation,
  out-of-transaction publication, catalogue finalization, and committed
  reconciliation within each live invocation. The advisory lock is not durable
  across process, connection, or database restart, and reacquisition does not
  prove an older callback quiesced. The retained operation, attempt claim, and
  active session reservation prevent another publisher. Same-operation
  recovery stays source-free and read-only until the exact committed journal
  state is verified, so a pre-commit phase fails closed even if verification
  overlaps an older callback.
- Publication never runs inside a database transaction. Successful
  finalization atomically revalidates the same session, operation, reservation,
  attempt, and claims; writes or confirms the exact path-free catalogue
  completion; retires the operation; releases the reservation; clears the
  active pointer; and writes the terminal anchor.
- Capture does not change the canonical attachment, lease, lifecycle, or
  writer epoch. Database expiry and epoch state remain logical admission
  evidence rather than physical fence proof.
- Dispatch or finalization uncertainty retains the durable operation,
  reservation, attempt, and physical evidence. The normal path never retries
  publication with a new attempt or reuses the stopped-writer capability.
- Committed capture reconciliation takes only the original checkpoint and
  mutation request. It loads the exact non-tombstoned durable attempt and asks
  the backend to verify only its already committed artefact, without a mutable
  source, writer, lease, attachment, clock, or stopped-writer capability.
  `prepared` and `materialized` publication never become success through this
  path.
- Historical committed-operation reads require the current session to retain
  the operation's exact immutable document identity and session-incarnation
  `createdAt`, with a revision no earlier than the operation's terminal
  revision (`expectedSession.revision + operation.revision + 1`). A matching
  identity restored from before the checkpoint commit, or a newer revision
  from another identity/incarnation, cannot authorize the historical attempt
  or catalogue.
- Production retains capture claims permanently. Any retained tombstone is
  non-authorizing and rejects reuse even if matching historical bytes,
  operation fields, or checkpoint metadata reappear from a session-volume
  restore.
- Restore is rejected by the production checkpoint authority. A later slice
  must define one canonical detached destination generation, finalize its
  exact restore result, and bind that generation to launcher admission before
  writable use.

## Next Steps

- Implement a bounded operational recovery enumerator and service loop for
  retained `starting` or `uncertain` captures. It must recover the exact
  checkpoint and mutation request from durable operation state and leave
  guard-busy or unverifiable attempts pending.
- Implement canonical restore destination generations and logical launcher
  admission around the measured-image reservation.
- Add an ext4 or filesystem-image physical backend before differential export,
  content-addressed storage, retention, and cross-host recovery work.
- Require authority-ledger disaster recovery to preserve or replay every claim
  and tombstone newer than retained artefact or volume generations; otherwise
  fence and rekey affected namespaces before reopening admission.
- Keep the read-only Git Summary separate from checkpoint correctness.

## Evidence

- `README.md`
- `migrations/authority/001-session-authority.sql`
- `src/postgres-checkpoint-mutation-authority.mjs`
- `src/postgres-operation-guard.mjs`
- `src/postgres-session-authority.mjs`
- `src/stopped-directory-backend.mjs`
- `src/stopped-directory-publication.mjs`
- `test/postgres-checkpoint-mutation-authority.test.mjs`
- `test/postgres-operation-guard.test.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/PROJECT_STATE.md`
- `docs/PROJECT_TODO.md`
