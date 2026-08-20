---
id: 20260820-c7e4a1
title: Provider-State PostgreSQL Operation Index Foundation
status: completed
created: 2026-08-20
updated: 2026-08-20
branch: wip/provider-state-operation-index
pr:
supersedes: []
superseded_by:
---

# Provider-State PostgreSQL Operation Index Foundation

## Summary

- Added migration 010's permanent PostgreSQL representation for exact
  filesystem-image-provider operation history.
- Added a separate state-authority adapter that advances the external head and
  matching prepared or committed record in one serializable durable cut.
- Kept production version 2 provider-state serving unchanged. The version 3
  adoption and local-history removal remain the next independent slice.

## Current State

- `session_authority.filesystem_image_provider_operations` is keyed by
  `(provider_id, anchor_id, operation_id)` and references the external provider
  head. It stores bounded canonical UTF-8 bytes rather than PostgreSQL JSON
  text, plus a domain-separated record digest and explicit operation, storage,
  logical-revision, prepared-checksum, and committed-checksum provenance.
- A row starts with the complete prepared checkpoint record. Its prepared
  prefix is immutable; it may gain the exact committed record, revision,
  checksum provenance, and digest once. Native commits use `indexed-frame-v1`
  with an exact checksum. A future adoption may use
  `unavailable-adopted-v2` with a null checksum only after the parent is a
  covering version 3 checkpoint. A committed row cannot be updated. A deferred
  delete guard rejects partial history removal while its anchor exists, while
  same-transaction operations-first anchor teardown remains available to tests
  and explicit administrative cleanup. A statement trigger rejects every
  `TRUNCATE`; no runtime deletion API is exposed. Migration 008 already requires
  every non-null value in the three head checksum columns to be an exact 64-byte
  lowercase-hex value. Migration 010 normalizes those valid values to
  `varchar(64)` before version 3 and defines all four operation checksum/digest
  columns with the same exact format.
- `createPostgresFilesystemImageProviderStateAuthority()` is separate from the
  existing exact two-method head anchor. It supports exact head-bound reads,
  C-collated bounded operation paging, append-prepared, append-committed, and
  rotation transitions. Append checksums must equal `nextHead.lastChecksum`;
  the prepared record also repeats that checksum. Page reads accept no more
  than four operations and each canonical record is capped at 4 MiB.
- The adapter performs the complete head compare-and-swap first, then applies
  the matching operation mutation in the same serializable transaction. A
  stale head returns `false` without touching history. Any operation mismatch
  aborts the transaction and therefore rolls the head back. Commit
  acknowledgement loss remains explicit; exact head and operation readback can
  determine whether the joint durable cut exists.
- Migration 010 permits external head contract versions 2 and 3 and normalizes
  checksum column storage without changing existing version 2 logical head
  values or contract versions. It does not enable a version 3 writer.
  Production version 2 checkpoints still retain all operations, so this
  foundation alone does not reduce local storage.

## Safety Boundary

- PostgreSQL is an external replay index, not physical ext4 authority. A head,
  operation row, or digest cannot independently authorize image mutation.
- Canonical bytes are revalidated before use. Explicit SQL metadata and stored
  digests are cross-checks; PostgreSQL does not reserialize JSON into replay
  identity.
- The foundation has no safe deletion floor. Every prepared and committed
  operation remains permanent while its anchor exists, including operations
  no longer referenced by current storage.
- Existing version 2 local state remains authoritative for production serving.
  No caller may remove local history merely because the additive index exists.
- The version-3-head/checkpoint predicate is an at-rest consistency gate, not
  proof that a row came from a validated checkpoint. The next slice must still
  validate the complete v2 checkpoint and active tail, prove import uniqueness
  and revision coverage, and commit all rows with the covering head atomically.

## Next Steps

- Implement the version 3 provider-state switch. Under the provider lock,
  validate complete version 2 checkpoint and log history, import it atomically
  into an empty PostgreSQL index while advancing the head, and resolve commit
  acknowledgement loss by exact readback. The version 3 checkpoint must cover
  every unavailable legacy suffix before that suffix becomes readable.
- Make version 3 checkpoints retain current storage and destroyed tombstones
  while serving arbitrary-age exact operation replay from PostgreSQL. Preserve
  and validate each current attachment's origin operation.
- Keep Git Summary deferred; it is not provider-state replay authority.

## Evidence

- Migration and store wiring:
  `migrations/authority/010-filesystem-image-provider-operations.sql`,
  `src/postgres-serializable-store.mjs`
- State authority:
  `src/postgres-filesystem-image-provider-state-authority.mjs`
- Focused coverage:
  `test/postgres-serializable-store.test.mjs`,
  `test/postgres-filesystem-image-provider-state-authority.test.mjs`
- Real PostgreSQL coverage: `integration/postgres-session-authority.mjs`
