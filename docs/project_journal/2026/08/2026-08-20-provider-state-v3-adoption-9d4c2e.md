---
id: 20260820-9d4c2e
title: Provider-State Version 3 Adoption
status: completed
created: 2026-08-20
updated: 2026-08-20
branch: wip/provider-state-v3-adoption
pr:
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
  changes only the head contract. Native version 3 writes continue to use
  `indexed-frame-v1`; an adopted prepared prefix may later receive that native
  committed suffix.
- `createPostgresFilesystemImageProviderStateRuntimeAuthority()` has the exact
  contract-version-2 surface needed for version 3 reads, bounded complete
  prepared paging, and normal head-plus-operation transitions. The older
  contract-version-1 authority and two-method head anchor remain explicit
  version 2 compatibility paths rather than silently changing their genesis.
- Cold version 3 load proves exact two-way equality between local prepared
  records and PostgreSQL's prepared pages. It validates every attached current
  storage against the named committed `attach` or `restore-attach` origin and
  rereads the exact head after projection checks. Arbitrary-age
  `readOperation()` results come from PostgreSQL.
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
  range requires a future streaming or paged adoption contract.
- The filesystem candidate is durable before the database cut. A definite
  unchanged outcome removes only that candidate; an uncertain outcome never
  guesses which generation is authoritative.
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
- Real PostgreSQL coverage: `integration/postgres-session-authority.mjs`
