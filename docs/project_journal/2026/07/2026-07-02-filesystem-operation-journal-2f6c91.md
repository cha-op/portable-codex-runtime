---
id: 20260702-2f6c91
title: Durable Filesystem Operation Journal
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/stopped-directory-durability
pr: 8
supersedes: []
superseded_by:
---

# Durable Filesystem Operation Journal

## Summary

- Added a canonical host-local operation journal with strict
  `absent -> prepared -> materialized -> committed` ordering.
- Added exact idempotent state and committed-result replay through copy-on-write
  record publication, identity-pinned file fsync, held-lock rename,
  parent-directory fsync, and canonical readback.

## Current State

- The first prepared record fixes the operation binding, request, complete
  checkpoint descriptor, and final `{ checkpoint, mutation }` result,
  including proof ID and status.
- Materialized and committed records preserve the predetermined result
  byte-for-byte, while exact retries return the canonical record without
  rewriting it or moving state backwards.
- Each journal instance pins its canonical directory identity across calls,
  while publication retains the fsynced temporary file descriptor through
  rename and identity-bound readback.
- Before rename, publication rechecks the exact predecessor inode and bytes (or
  confirmed absence) after all hooks and callbacks; the default lock holder
  repeats the destination identity check immediately before rename, so a
  callback cannot silently overwrite or roll back a changed canonical record.
- Restarted processes can read the last canonical journal phase and replay a
  committed exact result when no retained temporary record blocks inspection.
  A pre-rename temporary record fails closed with `journal_recovery_required`
  until a trusted operator recovery path resolves it; this slice exposes no
  public cleanup API.
- Each operation uses one deterministic temporary pathname, so retained
  recovery evidence is detected by direct lookup instead of repeatedly
  scanning the journal's permanent history.
- Post-rename, parent-sync, or readback uncertainty is reported as
  `journal_commit_outcome_uncertain` with `commitState: "uncertain"`.
  Lock-release failures use `journal_lock_release_failed` and preserve the
  prior `"not-committed"`, `"uncertain"`, or `"committed"` classification.
- Public journal failures are frozen `OperationJournalError` values with a
  fixed code, tri-state `commitState`, and `retryable: false`; the canonical
  schema version is exported as `OPERATION_JOURNAL_RECORD_VERSION`.
- Calls are process-locally serialized by canonical journal directory, future
  record versions are classified before applying the v1 schema, and generic
  token/API-key fields plus recognised provider credential forms are rejected
  before publication.
- The journal does not prove physical materialisation, writer stop, fence
  authority, publication, destination isolation, NFS guarantees, or backend
  success.

## Next Steps

- PR #10: add the same-process stopped-writer capability coordinator.
- PR #11: compose the journal, publication layer, and capability into the
  stopped-directory backend conformance slice.

## Evidence

- `docs/architecture/filesystem-operation-journal.md`
- `src/filesystem-operation-journal.mjs`
- `src/advisory-lock.mjs`
- `src/advisory-lock-holder.mjs`
- `test/filesystem-operation-journal.test.mjs`
- `test/advisory-lock.test.mjs`
