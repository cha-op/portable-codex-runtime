---
id: 20260829-c4e7a2
title: Atomic Crash-Capture Provider Catalogue
status: completed
created: 2026-08-29
updated: 2026-08-29
branch: wip/atomic-crash-capture-provider-catalogue
pr:
supersedes: []
superseded_by:
---

# Atomic Crash-Capture Provider Catalogue

## Summary

- Added an independent PostgreSQL catalogue for the private version 1 atomic
  crash-capture contract. It binds one exact request and provider plan to four
  separately unique opaque identities and grants at most one clearly committed
  physical dispatch.
- Added a dormant stopped-only classic LVM snapshot provider and fixed-command
  driver. The provider consumes durable catalogue admission before writer-stop
  authority and `lvcreate`, then retains and revalidates the read-only snapshot
  LV as the committed artifact.
- Kept the assembled ext4 backend, clean checkpoint facade, lifecycle facade,
  and public deployment unchanged.

## Current State

- Migration 12 creates `atomic_crash_captures` without lifecycle foreign keys
  or UUID aliases. Capture-attempt, operation, checkpoint, and artifact IDs are
  independently unique. Canonical request and provider-binding JSON are stored
  with SHA-256, while the committed result remains nullable until success.
- The only row transitions are `starting -> uncertain`,
  `starting -> committed`, and `uncertain -> committed`. Database triggers own
  the claim, uncertainty, and commit timestamps and reject caller-owned time,
  identity/request/binding/result replacement, reversal, delete, and truncate.
- Only a newly inserted row whose transaction outcome is known committed yields
  one process-local dispatch claim. Existing `starting` or `uncertain` rows and
  insert acknowledgement loss return no claim. Committed replay must match and
  revalidate the exact stored provider binding and result.
- The LVM wrapper delegates all seven lifecycle methods and changes only its
  private capability snapshot to `atomicPointInTimeCheckpoint: true`. Its
  deterministic binding records the origin LV UUID, snapshot name and tag, and
  COW allocation. COW allocation is deliberately distinct from the artifact's
  origin-visible block length.
- Fresh capture invokes one injected authority consumer with the exact opaque
  stopped-writer handle and request. Existing or uncertain attempts never call
  authority or `lvcreate`. A failed provider dispatch is durably uncertain;
  commit acknowledgement loss is never rewritten to uncertain.
- Source-free verification reads only committed catalogue state and the
  retained snapshot. It does not resolve or open the source root, consume
  authority, or dispatch another snapshot.

## Protected Properties

- Object identity is the snapshot LV UUID under
  `lvm-lv-uuid-v1`. Name, tag, and origin UUID bind the provider plan, while
  device-mapper UUID and major/minor are attachment observations for the same
  read window rather than substitutes for persistent object identity.
- Content stability is the exact LVM/block-device visible length plus a full
  streaming SHA-256. The driver observes the snapshot before and after hashing;
  a same-name replacement, size mismatch, digest mismatch, or unreadable
  observation is `unknown`.
- Access policy requires both read-only LVM attributes and
  `blockdev --getro=1`. Stable identity and bytes cannot compensate for a
  writable artifact.
- Snapshot COW usage may change while remaining below 100 percent, and
  unrelated LV metadata may churn. Those benign transitions do not imply
  replacement, content mutation, or access-policy change; exhaustion or a
  selected-signal mismatch does.

## Safety Boundary

- The provider supports only an injected complete stopped-writer authority.
  It does not create, persist, or verify a physical stale-writer fence.
- No current ext4 deployment discovers this wrapper. The production capability
  remains `atomicPointInTimeCheckpoint: false` with manual fencing.
- The slice adds no crash-prefix tail repair, writable restore generation,
  higher-epoch writer admission, clean/public capture integration, snapshot
  deletion or retention policy, distribution, compression, or encryption.
- The real PostgreSQL and privileged LVM paths remain required CI evidence on
  Linux because the local macOS host has neither PostgreSQL nor LVM runtime.

## Follow-up

- Compose complete stopped-writer authority into a private recovery path, then
  add the separate physical-fence branch required for stale-writer takeover.
- Repair only on a detached writable generation and admit a new writer only
  after successful repair under a strictly higher canonical fencing epoch.
- Keep host/controller/drive cache-loss evidence and export, distribution,
  encryption, retention, registry trust, and remote transport separately
  scoped. Keep Git Summary deferred.

## Validation

- `node --check` passed for the new catalogue, provider/driver, and integration
  modules and their focused tests.
- Catalogue and LVM focused tests passed: 53 passed and the one privileged LVM
  integration case skipped by default on macOS. The LVM-only unit suite passed
  all 37 tests after also exercising whitespace-padded LVM report and
  `dmsetup --columns` fields.
- The changed serializable-store and runtime-controller suites passed 176
  tests, and the detached deployment suite passed 35 tests.
- The unfiltered repository suite ran 3,534 tests: 3,497 passed, 36 skipped,
  and the sole failure was the unchanged live-auth watcher after the host
  returned `EMFILE: too many open files, watch`.
- The complete suite then passed with only that exact watcher test excluded by
  name: 3,533 tests, 3,497 passed, 36 skipped, and zero failures.
- Real PostgreSQL catalogue transitions and privileged LVM capture are owned by
  their required Linux CI jobs because no local PostgreSQL, container, or LVM
  runtime is installed.
- Workflow YAML parsing and `bash -n` over all 30 run blocks passed. Local
  `shellcheck` was unavailable. Project-journal validation and
  `git diff --check` passed.

## Evidence

- Catalogue migration: `migrations/authority/012-atomic-crash-capture-catalogue.sql`
- Catalogue: `src/postgres-atomic-crash-capture-catalogue.mjs`
- LVM provider and driver: `src/lvm-atomic-crash-capture-provider.mjs`
- PostgreSQL integration: `integration/postgres-atomic-crash-capture-catalogue.mjs`
- Privileged LVM integration: `integration/lvm-atomic-crash-capture-provider.mjs`
- Architecture: `docs/architecture/atomic-crash-capture-extension.md`
