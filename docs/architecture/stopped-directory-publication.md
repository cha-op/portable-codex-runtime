# Stopped-Directory Publication

## Scope

The stopped-directory publication layer turns an already stopped portable
directory tree into one durably published local checkpoint artefact, or turns
one such artefact into one durably published detached restore directory. It
binds physical filesystem work to the durable operation journal without
claiming writer-stop, lease, or backend authority that belongs to later
layers.

This layer supports only a trusted local filesystem whose rename, file fsync,
directory fsync, and cache-coherence semantics have been approved by the host
adapter. NFS, other remote or shared filesystems, and unknown filesystem types
are rejected or remain outside the guarantee. A lock file on a shared mount is
not evidence of stale-writer fencing.

The layer consumes the reusable stopped-tree pathname, topology, copy, digest,
and guarded-cleanup rules. It adds the storage barriers, deterministic private
staging, atomic final-name publication, exact readback, and journal ordering
that those primitives deliberately omit.

PR #10 owns authentication of a one-use same-process stopped-writer
capability. PR #11 owns the canonical fence recheck, attachment and destination
state, backend mutation envelope, and snapshot-core composition. This layer
must not accept an arbitrary object or callback as proof that a writer stopped,
that a restore destination is detached, or that a fence is current.

## Publication Objects

Checkpoint capture publishes one immutable bundle directory:

```text
<checkpoint artefact>/
├── artifact.json
└── payload/
    ├── codex-home/
    ├── workspace/
    └── .portable-runtime/
```

`artifact.json` is canonical, secret-free metadata. It binds its schema
version, the complete checkpoint descriptor fixed by `journal.prepare()`, the
portable payload digest and digest algorithm, and the payload shape. Its own
canonical byte digest is also fixed in materialisation metadata. It does not
contain a host path, attachment record, lease, credential, Git Summary, or
staging identity. The payload digest covers the same portable tree model as the
stopped-tree digest: entry paths and types, regular-file bytes, portable
permission bits, and supported symlink targets.

Restore validates the bundle manifest and copies only `payload/`. Its final
published directory is the restored payload tree itself; `artifact.json` is
not inserted into the session filesystem. Checkpoint bundle verification
requires the root to contain exactly `artifact.json` and `payload/`; an extra
root entry is committed-state corruption rather than ignored metadata.

Restore does not trust the bundle to authenticate itself. The caller must
supply the capture operation ID, portable payload digest, and canonical
manifest-byte digest from a trusted committed capture record or catalogue.
Those fields form the `artifactProof`, are bound into the restore journal
record, and must match both `artifact.json` and the payload before any restore
candidate is created. Simultaneously changing payload bytes and the manifest's
self-reported digest therefore cannot create a valid restore input. PR #11 is
responsible for obtaining this proof from canonical backend state rather than
from worker-controlled storage.

The trusted adapter supplies an absolute final path whose direct-child name is
validated and durably bound to the exact operation, target, and pinned parent
identity. The private staging name is a deterministic collision-resistant hash
of the validated operation ID and that final name. A staging name is never a
portable checkpoint reference and must never be exposed to a worker.

## Filesystem Authority

The publication root is a current-user-owned, mode `0700`, extended-ACL-free
directory with a trusted ancestor chain. The implementation opens and pins the
root by canonical path plus device/inode identity and revalidates its held
directory handle around every mutation and callback. Source and publication
owned roots must be distinct and non-nested. Both roots must also be disjoint
from the journal authority in either ancestor direction. Source, staging,
final, journal, and lock locations must not otherwise be symlink aliases,
hard-link aliases, or declared bind-mount aliases in a way that collapses
their roles. Before journal preparation, publication inventories the complete
source tree and rejects either the target-root or journal-root identity at any
source descendant, so an external bind-mount alias cannot turn destination
creation or journal advancement into a source mutation.

The operation journal exposes its pinned canonical directory and device/inode
identity only to this physical layer. Publication rejects a journal directory
that is the capture source, lies within the captured source, or lies within a
retained staging/final tree. Publication rejects every absolute symlink because
an external path component can change after validation and because host paths
are not portable; relocatable relative symlinks remain supported. The reusable
copy primitive also supports explicit path and device/inode deny authorities
for callers that retain its compatible absolute-link behavior. The journal's
approved local-filesystem profile and root device/inode are fixed in the
operation binding so a retry cannot silently move the journal to different
storage. The absolute journal path is used only for local topology checks and
is never persisted in the artefact or operation binding.

All operations under one publication-root identity are serialized in-process
and cross-process by one protected publication lock. This is required even for
different operation IDs: two operations can otherwise target the same
checkpoint artefact or restore destination. The fixed lock ordering is:

```text
publication-root lock -> filesystem operation journal lock
```

No code path may acquire these locks in the opposite order.

The public call synchronously validates and defensively snapshots the complete
coordinator binding, storage request, checkpoint result, and restore proof
before its first asynchronous publication-root lookup or queue wait. Caller
mutation after invocation therefore cannot change the later durable journal
record.

The source and destination trees reject nested mount points. A backend that
permits the declared source root itself to be a mounted volume must distinguish
that approved root mount from mounts below it and must pin the applicable mount
namespace and filesystem profile. Device equality alone does not prove that
two paths share safe rename or durability semantics.

For restore, the final pathname must be absent and must remain outside worker
admission while staging and commit are in progress. It must not identify an
active attachment or any capture source. The private staging pathname and a
visible-but-not-yet-committed final pathname are not launch authority. A
launcher or consumer must require the exact committed journal record before it
uses the final path.

## Ordered State Machine

One publication attempt holds the publication-root lock and pinned directory
authority through the complete sequence:

1. validate the exact operation, local filesystem profiles for the source,
   target, and journal, private roots, source, final target, and current
   recovery topology;
2. call `journal.prepare()` to durably fix the operation binding, storage
   request, checkpoint descriptor, and predetermined exact result before any
   physical materialisation begins;
3. establish the stopped source storage barrier by opening and validating the
   tree, fsyncing regular files, and fsyncing directories in post-order so each
   directory is synced only after its descendants; symlink entries are made
   durable by their containing directory; recheck that the source pathname
   still names the caller-observed root inode both before and after the barrier,
   and require that same identity when the copy opens its source root;
4. create the operation's deterministic private staging directory with
   exclusive creation and copy either the stopped source into a checkpoint
   bundle or the validated bundle payload into a restore tree;
5. fsync every staged regular file and then every staged directory in
   post-order, fsync the staging root, and fsync the held publication parent so
   the complete unpublished staging name and contents are durable;
6. recompute and compare the exact source, staged payload, and manifest
   digests and revalidate the held staging identity; after every callback that
   can observe the candidate, repeat the pinned identity check, complete staged
   tree fsync, publication-parent sync, exact bundle-shape/manifest/payload
   readback, and pinned identity check before calling
   `journal.markMaterialized()` with the fixed digests and device/inode
   identity encoded as canonical decimal strings; `materialized` therefore
   means a complete, durable, unpublished staging object, not merely that copy
   returned;
7. revalidate the publication root, lock, held staging inode, and absent final
   destination; after the last pre-rename callback, reassert the lock and repeat
   the full staged-tree fsync, publication-parent sync, exact readback, and
   pinned identity checks, then ask the trusted lock holder to atomically rename
   staging to final with an absent-destination precondition;
8. prove that the final pathname names the held staged inode, fsync the held
   publication parent, and perform exact held-inode, payload-digest, and
   `artifact.json` readback; restore readback verifies the payload digest and
   also verifies it against the source bundle manifest and trusted
   `artifactProof`;
9. call `journal.commit()` with the exact materialisation metadata and wait for
   its durable canonical readback, after repeating final-tree fsync,
   publication-parent sync, identity, and exact digest readback once all
   fault/test callbacks have returned; and
10. only after the committed record is visible may a consumer replay the
    result or a launcher admit the restore destination.

The source remains externally stopped throughout capture. The publication
layer detects many identity and metadata changes, but those checks are not a
substitute for the PR #10 stopped-writer capability or the PR #11 atomic fence
recheck.

The staged tree, its manifest, and the final tree are never deleted
speculatively after a failure. Retained objects are recovery evidence. Cleanup
is a separate trusted operator action that must re-open the private root,
inspect the journal, reject mount boundaries, and prove that the path still
names the intended recovery object.

## Journal and Physical State

The journal state and physical topology must be interpreted together:

| Journal state | Staging | Final | Meaning |
| --- | --- | --- | --- |
| `absent` | absent | absent | A fresh operation may start. |
| `absent` | present | any | Unowned evidence; trusted recovery is required. |
| `absent` | absent | present | The target is foreign or unexplained; it is never adopted. |
| `prepared` | absent | absent | Materialisation may restart only under fresh caller authority. |
| `prepared` | present | absent | The stage may be partial; normal publication fails with recovery required. |
| `prepared` | any | present | Publication may have escaped its recorded phase; recovery is required. |
| `materialized` | present | absent | Verify the recorded digest and identity before publishing under fresh authority. |
| `materialized` | absent | present | Rename may have completed; verify final, fsync its parent, and then commit. |
| `materialized` | present | present | The physical state is inconsistent; recovery is required. |
| `materialized` | absent | absent | The durable stage is missing; recovery is required. |
| `committed` | absent | present | Verify the exact final object and replay without copying or rewriting. |
| `committed` | present | any | Committed state contains unexplained recovery evidence and fails closed. |
| `committed` | absent | absent or invalid | The operation remains committed, but its published object is damaged or lost. |

An operation-journal temporary record has priority over this table. The
publication layer propagates its recovery or uncertainty classification and
does not infer a physical result from an incomplete journal transition.

Exact recovery never changes the operation ID, target, descriptor,
predetermined result, or materialisation metadata. A different operation ID
cannot adopt an existing final object even when its payload bytes happen to
match. Restore recovery additionally rejects a recorded materialisation whose
manifest or modeled digest no longer matches the trusted `artifactProof`, even
if an attacker supplied a staged tree with matching device/inode metadata.

## Failure and Commit Classification

Public failures are fixed, sanitized, frozen, and non-retryable. They expose no
filesystem path, collaborator exception, credential, file content, or prompt.
The publication `commitState` uses the same three values as the operation
journal:

- `not-committed` is used only when the trusted implementation proves that the
  final rename was not dispatched or did not occur;
- `uncertain` begins when final rename may have occurred and continues through
  final parent sync, exact final readback, and durable journal commit; and
- `committed` begins only after the exact committed journal record has passed
  canonical readback.

Validation, source-barrier, copy, stage-sync, digest, and final-precondition
failures before rename are not committed. A created staging object is retained
and makes later access require recovery.

Rename acknowledgement loss, a failure after rename dispatch, final identity
failure, parent-directory sync failure, final readback failure, or failure to
confirm the journal commit is uncertain. Once physical publication has
occurred, even a journal failure proven not to have advanced from
`materialized` does not make the overall storage operation
`not-committed`: the visible side effect requires reconciliation.

If a syntactically valid replay cannot open or resolve the source/publication
private roots before reading historical journal state, it is `uncertain`, not
`not-committed`: an unavailable root cannot prove that an earlier exact
operation did not commit. A non-absolute or non-normalized root remains a
deterministic caller error. A malformed persisted materialisation is journal
integrity failure while uncommitted and `published_state_invalid` when its
record is already committed; it is never reclassified as a caller argument
error.

A lock-release or final-handle-close failure preserves the prior commit state.
In particular, a failure after journal commit is a committed cleanup failure.
A later missing or corrupt final object also does not downgrade the historical
operation; it is committed-state corruption and must not be silently recreated
under the same operation ID.

Only the branded production rename path may classify its own explicit
absent-precondition or cross-device rejection as definitely not committed. An
injected collaborator cannot forge that classification.

## Atomicity and Same-UID Boundary

Consumers observe either the old namespace or the complete final directory at
the rename boundary. They must still consult the journal because a final path
can be visible while its record remains `materialized`.

The trusted lock holder repeats the absent-destination check immediately before
POSIX rename and the caller verifies the held inode immediately afterward.
This prevents cooperating publication processes from overwriting a target and
detects many pathname replacements. It is not a portable kernel
compare-and-swap. A non-cooperating process with the same UID can race the last
pathname check and the rename syscall. Covering that threat requires a
dirfd-relative kernel no-replace primitive, such as Linux
`renameat2(RENAME_NOREPLACE)`, a platform-equivalent primitive, or a distinct
OS identity. That final same-UID syscall race is an explicit non-guarantee of
this layer.

## Filesystem and Durability Boundary

The guarantee is limited to process interruption and host crash on a trusted
local filesystem that correctly implements the validated fsync and rename
contract. The implementation fails closed when directory fsync is unavailable
or the filesystem profile is remote, shared, or unknown.

This contract does not prove equivalent semantics for NFS, SMB, distributed
filesystems, object-store mounts, FUSE providers, remote caches, controller or
drive firmware that lies about flush completion, or storage that requires a
stronger platform-specific primitive than the one used. Such storage needs a
separate adapter with its own server-side idempotency, fencing, publication,
and fault evidence.

The layer implements stopped-writer `clean` directory capture and restore. It
does not implement an atomic live-volume `crash-prefix` snapshot, rollout-tail
repair, same-image Codex resume verification, differential compression,
content-addressed storage, encryption, retention, periodic long-goal capture,
cross-host migration, or Git Summary.
