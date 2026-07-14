# Reusable Stopped-Tree Primitives

## Scope

`src/stopped-tree.mjs` contains the reusable filesystem-validation, copy,
digest, and guarded-cleanup primitives that were originally embedded in the
interrupted-turn recovery probe. The extraction changes module ownership, not
filesystem semantics. The probe imports and re-exports its existing public
surface so current callers retain the same API and failure behaviour.

The probe-compatible public surface is:

- `parseLinuxMountInfo()`;
- `parseDarwinMountTable()`;
- `decodePortablePathBytes()`;
- `assertPortableDirectoryNames()`;
- `parseLinuxGetfacl()`;
- `inspectLinuxRecoveryAcl()`;
- `copyStoppedTree()`;
- `digestTree()`; and
- `removeTreeForCleanup()`.

The extracted module also exposes a small set of repository-internal metadata,
digest, and ACL helpers needed by the probe. Those helpers are not a new stable
storage-backend contract.

The stopped-directory publication layer additionally uses
`openStoppedTreeRootAuthority()`, `copyStoppedTreeBetweenRoots()`, and
`syncStoppedTree()`. The two-root copy retains a pinned authority for each
private root, rejects root aliases and nesting, and allows a trusted adapter to
declare the source root itself as its approved mount while still rejecting
nested mounts. A caller may also require the source root to retain an
already-observed device/inode identity, reject every absolute symlink, or deny
absolute-link traversal into explicit path and device/inode authorities. These
are publication-building primitives, not independent backend authority.
Cross-root copy inventories every source entry identity before it creates the
destination and rejects a destination-root identity found anywhere in that
inventory, including a descendant bind-mount alias.
`stoppedTreeContainsAnyIdentity()` provides the corresponding targeted proof:
it validates the mount table before recursion, permits an explicitly approved
root mount, rejects nested mounts and cross-device entries, stops before
opening a directory whose identity already matches, and repeats the mount
check after the scan. Its injectable mount-table reader is a trusted test and
platform-adapter seam.

These functions operate only after an external coordinator has stopped the
writer. They do not stop a process, authenticate stopped-writer evidence, or
authorise a checkpoint.

## Filesystem Contract

`copyStoppedTree()` requires a current-user-owned, mode `0700`,
extended-ACL-free owned root with a trusted ancestor chain. Its source and
destination are direct children of that root. The tree operations hold and
revalidate filesystem identities, reject mount boundaries, and use no-follow
opens around copied entries.

Portable trees contain snapshot-user-accessible regular files, directories,
and supported UTF-8 symlinks. The model preserves regular-file and directory
POSIX rwx permission bits, file bytes, relative paths, entry types, and symlink
targets. The digest covers that same model.

The implementation fails closed on unsupported names or aliases, inaccessible
entries, symlink traversal outside the permitted model, special permission
bits, hard links, sockets, FIFOs, devices, mount points, and detected identity
or metadata races. Ownership, ACLs, extended attributes, timestamps, symlink
permission bits, and hard-link topology are not preserved.

Concurrent mutation by another process with the same UID is outside this
security boundary. The caller must provide exclusive single-writer control.
If a copy fails after creating its destination, the partial destination is
retained; guarded cleanup never assumes that a pathname still names the object
it previously observed.

## Non-Durable Boundary

These are stopped-tree compatibility primitives, not a snapshot backend. They
do not provide:

- a storage or filesystem `fsync`/`syncfs` barrier;
- atomic checkpoint or restore publication;
- a durable operation journal or operation-ID replay;
- a durable checkpoint descriptor or backend proof;
- canonical lease, attachment, or fencing checks;
- restore destination isolation;
- crash-consistent capture of a live or fenced writer; or
- power-loss durability.

`digestTree()` detects differences within the portable tree model; a matching
digest is not evidence that either tree reached stable storage. Similarly,
successful `copyStoppedTree()` means that a stopped source was copied under
the helper's filesystem rules, not that an atomic or durable checkpoint was
published.

The separate filesystem operation journal can durably record predetermined
operation phases and results, but it does not by itself add a storage barrier
or publication semantics to these primitives. The stopped-directory
publication layer composes both boundaries for an approved local filesystem.
See `filesystem-operation-journal.md` and
`stopped-directory-publication.md`.

## Integration Boundary

The stopped-directory publication layer now composes these primitives with a
post-order fsync barrier, deterministic private staging, atomic final-name
publication, exact readback, and the durable operation journal. The primitives
remain non-durable when called independently.

PR #10 supplies the trusted same-process stopped-writer capability. PR #11
must compose that capability and this publication layer with an atomic
canonical fence recheck and return the exact descriptor and mutation envelope
required by the snapshot and restore core.

The ext4 or filesystem-image backend, differential export, retention,
encryption, periodic long-goal snapshots, cross-host verification, and Git
Summary remain separate workstreams.
