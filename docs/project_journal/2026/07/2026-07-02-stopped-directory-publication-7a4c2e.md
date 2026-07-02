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
- Journal topology is pinned, its approved local-filesystem profile,
  filesystem-incarnation ID, object-identity scheme, and root object ID are
  durably bound to the operation, while raw device/inode identities remain
  runtime-only guards. The trusted adapter must supply the stable filesystem
  and non-reusable object IDs because Node does not expose them. Its atomic
  object inspection also returns the runtime device/inode so the core can bind
  each object ID to the pinned object and reject inspection-time path ABA;
  publication-wide bidirectional checks reject either one object ID assigned
  to distinct visible objects or one object changing object IDs during an
  attempt, and the source-owned-root profile is separately durable. Source
  filesystem profile and object identity are re-read after every source
  barrier or observable callback before copying or committing. The destination
  root profile and object identity are likewise re-read at callback,
  materialisation, rename, and commit boundaries, with failures mapped to the
  already-discovered durable state.
  Publication also rejects all absolute source symlinks so mutable host aliases cannot
  redirect a portable artefact into the journal authority after validation.
- Public inputs are deeply snapshotted before queueing; source and publication
  roots are distinct and journal-disjoint; source root identity is preserved
  across the barrier and copy; and checkpoint replay requires an exact
  two-entry bundle root.
- Complete source-tree identity scans reject target or journal bind-mount
  aliases before journal preparation or destination creation. Candidate state
  is re-synced and exactly revalidated after its last callback and before the
  journal may advance to `materialized`.
- The last pre-rename and pre-commit callbacks are followed by complete tree
  and parent durability barriers plus exact readback. Unavailable private roots
  before journal discovery are uncertain while malformed root syntax remains a
  caller error, and malformed persisted materialisation is classified as
  journal or committed-state corruption.
- Prepared candidate-only inconsistencies remain recovery-required, while a
  prepared record with a visible final stays publication-uncertain because a
  complete-candidate callback may have published it. Prepared replay also stays
  uncertain when current authority cannot complete both topology probes.
  Durable destination filesystem, root object ID, final name, and candidate
  name continuity are proven before either probe may downgrade uncertainty, so
  a rebound caller root or name cannot hide an older final. Host
  adapters can inject the trusted ACL inspection capability used consistently
  by root pinning and copy.
- Read-only source/journal/target topology checks now run before publication
  lock creation and repeat while locked. Every injected callback is followed
  by pinned root-authority revalidation before further source reads or target
  writes.
- Missing or non-directory source leaves are classified only after historical
  journal discovery, so materialized/committed replay can use its recorded
  source binding. Restore repeatedly requires the checkpoint bundle root to
  contain exactly `artifact.json` and `payload/`. The binding now records the
  direct source-leaf filesystem-incarnation ID/object ID, so prepared replay
  rejects same-path leaf replacement while later phases reconstruct the
  durable binding without recopying the source. Retained-tree identity digests
  bind each relative path and entry kind to a trusted non-reusable object ID,
  not host-local or persistently reused inode numbers.
- Materialized recovery remains uncertain until current authority proves a
  candidate-only topology. The journal binds a complete retained-tree identity
  digest; recovery/readback rejects source-retained identity intersections,
  same-byte physical-object replacement, inode reuse, and candidate/final roots that no longer satisfy
  their owner, pinned-mode, and ACL policy. Checkpoint envelopes remain `0700`;
  restore payload roots retain their modeled mode inside a private `0700`
  destination authority.
- A pre-rename callback is treated as publication-uncertain until a held-lock
  probe proves the staged inode is not visible at the final path, preventing a
  callback-side rename from being misreported as definitely not committed.
- The same held-lock final-path proof now surrounds the complete-candidate
  callback, the journal's `materialized` transition callbacks, and the
  post-materialization callback, so callback-side publication cannot be
  misreported while the journal is `prepared` or `materialized`. After journal
  commit and its own fault callbacks return, publication repeats the full
  committed tree fsync, parent sync, candidate-absence, identity, mode/ACL, and
  digest barrier before reporting success.
- Journal prepare/replay calls are also treated as observable namespace
  boundaries: candidate/final presence and runtime identities are pinned before
  the call and must be unchanged afterward before a prior `not-committed`
  classification can be restored.
- Candidate roots are provisionally pinned immediately after creation, before
  `afterCandidateCreated` or `afterCopy` can observe them. Those early callbacks
  use the same held-final probe as later materialization callbacks. The lock
  name and complete `.publication-` staging prefix are reserved and cannot be
  selected as artifact or restore final names.
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
