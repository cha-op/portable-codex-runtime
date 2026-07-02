---
id: 20260702-7a4c2e
title: Stopped-Directory Publication
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/stopped-tree-publication
pr: 9
supersedes: []
superseded_by:
---

# Stopped-Directory Publication

## Summary

- Added the local-filesystem publication boundary that binds a stopped source
  barrier, deterministic private staging, exact bundle or restore-tree
  materialisation, atomic final-name publication, durable readback, and the
  filesystem operation journal.
- Defined checkpoint artefacts as an `artifact.json` plus `payload/` bundle;
  restore publishes only the payload tree.

## Current State

- Publication is ordered through a publication-root lock, journal prepare,
  post-order source fsync, staged copy and post-order fsync, parent sync,
  exact digest and identity materialisation, trusted absent-precondition
  rename, final parent sync and readback, and journal commit.
- A final path is not consumer or launcher authority until the exact journal
  record is committed. Staging and ambiguous final objects are retained as
  recovery evidence rather than deleted speculatively.
- Restore binds a trusted capture operation ID, payload digest, and canonical
  manifest digest from committed catalogue state; payload and manifest bytes
  cannot authenticate themselves. Materialized restore replay rechecks both
  recorded digests against that proof before trusting a retained stage.
- Journal topology is pinned, its approved local-filesystem profile and root
  identity are durably bound to the operation, and publication rejects all
  absolute source symlinks so mutable host aliases cannot redirect a portable
  artefact into the journal authority after validation.
- Public inputs are deeply snapshotted before queueing; source and publication
  roots are distinct and journal-disjoint; source root identity is preserved
  across the barrier and copy; and checkpoint replay requires an exact
  two-entry bundle root.
- Restart classification combines journal phase with deterministic staging and
  final topology. Rename, parent-sync, final-readback, and journal-commit
  uncertainty never downgrade to a pre-commit I/O failure.
- The implementation boundary is a trusted local filesystem. NFS, remote,
  shared, and unknown filesystem semantics are rejected or explicitly outside
  the guarantee.
- The trusted absent-destination check protects cooperating publishers, while
  the final syscall race from a non-cooperating process with the same UID
  remains an explicit non-guarantee without a kernel no-replace primitive.

## Next Steps

- PR #10: authenticate a one-use same-process stopped-writer capability bound
  to the exact writer incarnation, attachment, and fence.
- PR #11: compose publication, journal, stopped-writer capability, canonical
  fence checks, and backend mutation results into the stopped-directory
  adapter and conformance suite.

## Evidence

- `docs/architecture/stopped-directory-publication.md`
- `src/stopped-directory-publication.mjs`
- `test/stopped-directory-publication.test.mjs`
- PR #9
