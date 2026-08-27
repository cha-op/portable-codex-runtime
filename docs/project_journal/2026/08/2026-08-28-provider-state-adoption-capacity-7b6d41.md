---
id: 20260828-7b6d41
title: Provider-State Adoption Capacity
status: completed
created: 2026-08-28
updated: 2026-08-28
branch: wip/provider-state-adoption-capacity
pr:
supersedes: []
superseded_by:
---

# Provider-State Adoption Capacity

## Summary

- Added an explicit paged adoption contract version 2 for version 2 provider
  state beyond the full-array version 1 transport capacity.
- Preserved the exact version 1 manifest identity, migration 011 database
  authority, and durable adoption recovery state machine.

## Current State

- `createPostgresFilesystemImageProviderStateAdoptionAuthority()` and
  `POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_ADOPTION_AUTHORITY_CONTRACT_VERSION`
  remain the exact contract version 1 full-array surface. It still accepts at
  most 65,535 operations, 65,535 storages, and 64 MiB of aggregate canonical
  operation/prepared-projection/storage material, and fails an oversized
  request with `state_capacity_exhausted` before candidate mutation.
- `createPostgresFilesystemImageProviderStatePagedAdoptionAuthority()` and
  `POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_PAGED_ADOPTION_AUTHORITY_CONTRACT_VERSION`
  expose `{ contractVersion: 2, compareAndAdopt }`. Its exact request is
  `{ expectedHead, nextHead, operationPager, storagePager }`; each pager is
  `{ contractVersion: 1, readPage }`. Operation pages use
  `{ afterOperationId, limit } -> { operations, nextAfterOperationId }` and
  storage pages use
  `{ afterStorageId, limit } -> { storages, nextAfterStorageId }`.
- The authority fixes every page request at four items. Exclusive item-ID
  cursors make both streams restartable and bound each pager invocation. One
  `SERIALIZABLE` PostgreSQL transaction normalises the pages into `pg_temp`
  staging and replay relations created with `ON COMMIT DROP`, then drives the
  unchanged migration 011 import window and deferred coverage proof. Each
  transaction starts each pager from null exactly once; all later replay,
  import, and comparison work reads only the temporary relations. A
  serialization retry or acknowledgement-loss readback stages once again in a
  new transaction and checks its manifest IDs and counts against the retained
  constant-size first-attempt values, so source drift fails closed.
- Pager versions, cursor values, and page boundaries never enter manifest
  input. Every valid partition of the same canonical state therefore produces
  the exact version 1 manifest bytes.
- Candidate publication, the durable `pending` marker, COMMIT acknowledgement
  readback, uncertain preservation of both generations, projection validation,
  the durable `verified` marker, and cold source-cleanup recovery retain their
  existing semantics across both transport versions.

## Safety Boundary

- Version 2 removes only the adoption version 1 full-array transport limits.
  The 4 MiB per-record/frame-payload bound, active-tail 65,535-frame/64 MiB envelope,
  uint32 checkpoint-count limits, and version 3 runtime-projection limits remain
  distinct and unchanged.
- Migration 011 is reused without schema or trigger changes. The manifest
  remains a receipt rather than a write token, and PostgreSQL remains exact
  operation replay authority rather than physical ext4 mutation authority.

## Follow-up

- Keep power-loss/crash-prefix evidence, automatic stale-writer fencing,
  differential export/compression, encryption, registry trust, and remote
  transport in their already-separated clean/manual-fencing slices.
- Keep Git Summary deferred; it is not provider-state replay authority.

## Evidence

- Dependency: [PR #56](https://github.com/cha-op/portable-codex-runtime/pull/56)
- Implementation surfaces:
  `src/postgres-filesystem-image-provider-state-authority.mjs`,
  `src/filesystem-image-provider-state.mjs`
- Focused coverage surfaces:
  `test/postgres-filesystem-image-provider-state-authority.test.mjs`,
  `test/filesystem-image-provider-state.test.mjs`,
  `integration/postgres-session-authority.mjs`
