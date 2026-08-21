---
id: 20260820-9d4c2e
title: Provider-State Version 3 Adoption
status: completed
created: 2026-08-20
updated: 2026-08-20
branch: wip/provider-state-v3-adoption
pr: https://github.com/cha-op/portable-codex-runtime/pull/56
supersedes: []
superseded_by:
---

# Provider-State Version 3 Adoption

## Summary

- Added migration 011's transaction-bound version 2-to-3 adoption authority.
- Switched exact provider-state serving to version 3 checkpoints that retain
  current storage, destroyed tombstones, attachment origins, and live prepared
  recovery work, while PostgreSQL supplies permanent committed replay.
- Bound ext4-to-Podman attachment reconstruction to the exact committed attach
  or restore-attach origin operation.

## Current State

- Version 2 and version 3 heads, frames, and checkpoint-state digests use
  separate domains. The parser preserves the stored contract version and also
  requires each canonical payload sequence to equal its binary envelope
  sequence.
- A provider-locked version 2 reducer orders every prepared and committed event
  by uint64 revision, proves exact `1..stateRevision` coverage, enforces one
  pending operation per storage, replays complete canonical before-state
  lineage, and derives the exact final storages and attachment origins.
- Before PostgreSQL changes authority, the provider writes, syncs, reads back,
  and revalidates a covering version 3 checkpoint plus empty active log in the
  next generation. That checkpoint contains every live prepared record and
  every current storage wrapper. Destroyed storage remains a tombstone;
  committed records are not copied into the next checkpoint.
- `createPostgresFilesystemImageProviderStateAdoptionAuthority()` has the
  frozen one-method surface `{ contractVersion: 1, compareAndAdopt }`.
  Its deterministic manifest digest binds provider and anchor identities,
  source and target heads, every canonical operation and checksum-provenance
  choice, final storages, and attachment-origin IDs. Callers never provide the
  manifest ID or PostgreSQL transaction ID.
- Migration 011 records the manifest and database-supplied xid on the exact
  covering version 2-to-3 rotation. Only that transaction may import tagged
  rows or write the unavailable legacy committed-checksum provenance. Its
  deferred trigger compares the final stored head with the adoption event and
  proves that prepared and committed revisions together contain each revision
  from one through the checkpoint boundary exactly once. A copied manifest,
  head-only update, partial import, duplicate, extra row, cross-manifest row,
  second same-transaction head advance, or later unavailable suffix rolls the
  transaction back. Same-column unique indexes, cross-column revision guards,
  and head-bound revision ceilings prevent an operation from being added after
  an explicit early execution of the deferred coverage trigger.
- Legacy null-marker adoption requires an initially empty operation index and
  imports every row in fixed 64-row batches. An already-indexed version 2 head
  requires the complete existing rows to equal the validated candidate and
  changes only the head contract; its source and target history are each
  revalidated through one ordered row stream. Native version 3 writes continue
  to use `indexed-frame-v1`; an adopted prepared prefix may later receive that
  native committed suffix.
- `createPostgresFilesystemImageProviderStateRuntimeAuthority()` has the exact
  contract-version-3 surface needed for version 3 reads, bounded diagnostic
  prepared paging, compact projection comparison, and normal
  head-plus-operation transitions. The older
  contract-version-1 authority and two-method head anchor remain explicit
  version 2 compatibility paths rather than silently changing their genesis.
- Cold version 3 load computes compact domain-separated summaries for all local
  prepared records and attached-storage origins. In one serializable
  transaction, PostgreSQL reads the exact head and independently streams and
  fully normalizes the complete prepared set through one data `SELECT` whose
  `LIMIT` comes from that head's structural bound. Attachment-origin input and
  each named committed `attach` or `restore-attach` query result are validated
  as independent fixed batches of at most 65,535 IDs. For `A` origins, the
  projection phase therefore uses one prepared data `SELECT` plus
  `max(1, ceil(A / 65,535))` streamed origin data `SELECT` statements, in
  addition to the exact-head `SELECT`; SQL parameters and additional memory
  remain bounded per batch. The frozen origin array is preflighted and hashed
  sequentially with constant authority-owned working memory, then normalized
  again only for the current query batch. It returns a receipt only for an
  exact match under the exact head. The repository-pinned
  `pg@8.22.0` portal fetches 1,024 rows at a time; the store accepts completion
  only after exact `SELECT` command, row-count, empty-result-accumulator, and
  transaction-identity checks. A server `ErrorResponse` sends one protocol
  `Sync` before rollback waits for `ReadyForQuery`; a client-side query timeout
  or failed sync instead destroys the dedicated connection so rollback cannot
  remain queued behind an unrecoverable active query.
  The provider caches that receipt only for the same authority instance, head,
  and unchanged loaded generation; cold reparsing, adoption, and uncertain
  acknowledgement readback revalidate it. Arbitrary-age `readOperation()`
  results come from PostgreSQL.
- Once an exact append CAS has advanced the PostgreSQL head and operation row,
  any later projection query or receipt failure preserves its specific error
  code but reports `commitState: "committed"`; callers cannot safely retry an
  already durable user operation as uncommitted.
- The version 2 ext4-to-Podman persistent binding requires
  `currentAttachmentOriginOperationId` to equal the queried operation. It
  compares the origin committed storage with current storage after normalizing
  only a legal monotonic storage-revision increase, while retaining the full
  attachment, writer, data-root, mount, and repeated-view checks.
- Adoption acknowledgement loss has three outcomes. Exact target head,
  completeness marker, manifest, provenance mode, and complete operation rows
  prove success. Exact unchanged source plus its original row state proves no
  commit. Any third head or mismatched, missing, extra, or unreadable evidence
  preserves both filesystem generations and reports
  `commit_outcome_uncertain`.
- The filesystem records each in-flight adoption with canonical,
  generation-scoped `pending` and `verified` markers. Each phase is bound into
  the canonical bytes, written and synced under a staging name, then atomically
  published and directory-synced. A cold `pending` recovery reads both
  retained generations without truncation, reconstructs the complete version
  2 operation manifest and target storage projection, and asks PostgreSQL to
  verify or replay that exact request. Only an exact successful authority
  result plus projection validation may create `verified`; the version 2
  source is deleted only after that marker is durable. Verified recovery
  replays the exact manifest while the complete source remains, and safely
  finishes an interrupted source cleanup when only part or none of that source
  remains. A marker/head mismatch, marker tamper, or a version 3 head with an
  unmarked version 2 predecessor fails closed and preserves the recovery
  material. Ledger-only predecessors, including generation zero, are decoded
  by their delta sequence and revision fields rather than a checkpoint-only
  generation field that delta frames do not contain.
- A permanent lifecycle registry owns each `(provider_id, anchor_id)` key.
  Every head insert claims that same unique row, while complete teardown of any
  operation history moves it from active to immutable retired. This real
  database conflict blocks both a same-transaction reinsert after an early
  deferred delete check and a concurrent recreation. Every head deletion,
  including an empty head, retires the identity to prevent durable-anchor ABA.
- An internal event-revision registry owns one unique key for every prepared
  and committed revision. Migration validates each existing non-null marker;
  subsequent indexed heads may advance by only one revision whose event exists
  at transaction completion, or rotate without advancing. Adoption retains its
  separate full-range deferred proof, so raw head inserts and revision jumps
  cannot manufacture a complete permanent index. Stored heads exclude revision
  zero, and a database-managed progress transaction ID prevents an early
  constraint check from authorizing a second head mutation in the same
  transaction.

## Safety Boundary

- The manifest is a durable receipt, not a write token. The database xid opens
  only one transaction-local import window, and the deferred coverage proof is
  still independent of the provider's semantic reducer.
- Local version 3 checkpoints retain unresolved prepared operations because
  they are recovery working state. They never retain committed history. The
  working set is bounded by live storage work, not by the number of completed
  operations, but it is not globally constant.
- The full-array adoption version 1 request accepts at most 65,535 operations,
  65,535 storages, and 64 MiB of aggregate canonical
  operation/prepared-projection/storage material. A valid version 2 state
  outside those operational limits fails with `state_capacity_exhausted`
  before candidate cleanup or creation. Supporting the wider version 2 format
  range requires a future streaming or paged adoption contract. These limits
  apply only to adoption version 1; a legal version 3 runtime projection is
  instead bounded by its exact stored head and streamed attachment-origin
  batches, not by the adoption envelope.
- Runtime projection reads and validates the exact stored head before it
  enumerates or canonicalizes caller-supplied attachment origins. A stale or
  forged expected head therefore cannot select a larger normalization bound or
  force caller-scale allocation before the mismatch is returned.
- The filesystem candidate and `pending` marker are durable before the
  database cut. A definite unchanged outcome removes only that candidate and
  its marker; an uncertain outcome never guesses which generation is
  authoritative. After exact success and projection validation, a durable
  `verified` marker protects the source-cleanup crash window; cold recovery
  revalidates the projection before completing source or marker cleanup.
- PostgreSQL is exact replay authority, not physical ext4 mutation authority.
  The provider lock, filesystem descriptors, external head, canonical storage
  lineage, and ext4/Podman observation boundary remain separate requirements.

## Follow-up

- Add a versioned streaming or paged adoption contract for version 2 state
  beyond the full-array operational capacity.
- Keep power-loss/crash-prefix evidence, automatic stale-writer fencing,
  differential export/compression, encryption, registry trust, and remote
  transport in separate slices.
- Keep Git Summary deferred; it is not provider-state replay authority.

## Evidence

- Migration and authority:
  `migrations/authority/011-filesystem-image-provider-state-v3-adoption.sql`,
  `src/postgres-filesystem-image-provider-state-authority.mjs`
- Provider format and runtime:
  `src/filesystem-image-provider-state.mjs`
- Attachment-origin binding:
  `src/ext4-podman-attachment-binding.mjs`
- Focused coverage:
  `test/postgres-serializable-store.test.mjs`,
  `test/postgres-filesystem-image-provider-state-authority.test.mjs`,
  `test/filesystem-image-provider-state.test.mjs`,
  `test/ext4-podman-attachment-binding.test.mjs`
- Real-PostgreSQL integration fixture (requires a configured database):
  `integration/postgres-session-authority.mjs`
