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
The exact `.stopped-directory-publication.lock` name and the complete
`.publication-` staging prefix are reserved implementation namespaces and are
rejected as caller-selected final names before lock or journal mutation.

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
Mount-table boundaries are checked before and after each recursive identity
scan; the declared tree root may itself be an approved mount, but any nested
mount fails before inventory begins. Publication always inventories the
journal tree for source-root and target-root identities; `absent` and
`prepared` states also check the current source-leaf identity. Each scan stops
before descending into a matching directory. This rejects a publication root
exposed as a journal descendant through a bind alias without walking the
aliased publication tree, while `materialized` and `committed` replay need not
inspect a caller's replacement source leaf.

The operation journal exposes its pinned canonical directory and runtime
device/inode identity only to this physical layer. Publication rejects a journal directory
that is the capture source, lies within the captured source, or lies within a
retained staging/final tree. Publication rejects every absolute symlink because
an external path component can change after validation and because host paths
are not portable; relocatable relative symlinks remain supported. The reusable
copy primitive also supports explicit path and device/inode deny authorities
for callers that retain its compatible absolute-link behavior. The journal's
approved local-filesystem profile, trusted filesystem incarnation ID, object
identity scheme, and root object ID are fixed in the operation binding so a
retry cannot silently move the journal to different storage. Raw `st_dev` and
`st_ino` values remain runtime-only because they can change across host
attachment or remount. The absolute journal path is used only for local
topology checks and is never persisted in the artefact or operation binding.

The trusted filesystem adapter must supply a `filesystemId` that identifies the
filesystem incarnation across hosts and remounts, changes after reformat or an
independent writable clone, and is not derived from a mount ID, device number,
device pathname, or worker input. Node `statfs` does not expose such an ID, so
the built-in inspector fails closed and production hosts must inject one (for
example a filesystem/volume UUID or a control-plane-protected incarnation
marker). The adapter must also name its `objectIdentityScheme` and provide an
opaque `objectId` for every inspected object. An object ID must remain stable
for the same object across process restart, host attachment, rename, and
remount, but must change when an inode is deleted and reused. It must not be
derived from path, inode, ctime, content, or worker input alone. A native object
handle or inode-generation identity should be domain-separated and hashed
before persistence if the raw value could grant authority. The same atomic
adapter primitive must return a data-only snapshot of the current runtime
device/inode with the object ID; getters, setters, symbols, and extra fields are
rejected. The core compares that pair with its held/path pin so a path cannot
be temporarily swapped to an older object for inspection and then swapped
back. Durable physical identities are `filesystemId + objectIdentityScheme +
objectId`; runtime path, fd, alias, and rename
guards continue to use the current mount's `st_dev + st_ino`. If either stable
identity capability is unavailable, publication fails closed before prepare.

All operations under one publication-root identity are serialized in-process
and cross-process by one protected publication lock. This is required even for
different operation IDs: two operations can otherwise target the same
checkpoint artefact or restore destination. The fixed lock ordering is:

```text
publication-root lock -> filesystem operation journal lock
```

No code path may acquire these locks in the opposite order.

The trusted publication-root provisioner must create the exported
`.stopped-directory-publication.lock` path before the root is exposed to a
worker or used as a publication target. It must be an empty regular file owned
by the runtime UID, have one link and exact mode `0600`, and be made durable by
fsyncing both the file and its parent directory. Existing roots are upgraded
only while detached and quiescent. The publish path acquires this inode with
`requireExisting: true`: it never creates, chmods, truncates, unlinks, or
repairs the lock path. On macOS the `lockf` executor also uses `-n`, so a
concurrent unlink cannot recreate the path. A missing or unsafe provisioned
lock fails closed as `publication_outcome_uncertain`.
An injected `acquireLock(path, { requireExisting: true })` implementation is a
trusted adapter and must preserve the same no-mutation and existing-inode
contract rather than ignoring the option.

The trusted storage adapter maps each publication-owned operation target to
one publication root; callers cannot select an alternate root for the same
operation. Every journal transition for such an operation goes through this
publication layer while the mapped root lock is held. Direct journal
transitions or alternate-root attempts are outside the protocol because they
would bypass the state-hint serialization boundary.

The public call synchronously validates and defensively snapshots the complete
coordinator binding, storage request, checkpoint result, and restore proof
before its first asynchronous publication-root lookup or queue wait. Caller
mutation after invocation therefore cannot change the later durable journal
record.

The source and destination trees reject nested mount points. A backend that
permits the declared source root itself to be a mounted volume must distinguish
that approved root mount from mounts below it and must pin the applicable mount
namespace and filesystem profile. Device equality alone does not prove that
two paths share safe rename or durability semantics. The same injected mount
inspector governs copy, sync, modeled digest, persistent-identity digest, and
source/publication disjointness checks for source, candidate, and final trees;
none of those checks may fall back to a different host mount table.

For restore, the final pathname must be absent and must remain outside worker
admission while staging and commit are in progress. It must not identify an
active attachment or any capture source. The private staging pathname and a
visible-but-not-yet-committed final pathname are not launch authority. A
launcher or consumer must require the exact committed journal record before it
uses the final path.

## Ordered State Machine

One publication attempt first performs a read-only root-only topology
preflight, then holds the publication-root lock and pinned directory
authorities through the durable-state preflight and remaining sequence:

1. validate the exact operation, journal profile, private roots, direct source
   location, final target, and root-only recovery topology; acquire the
   preprovisioned publication lock without changing its inode or metadata, then
   repeat root-only authority and topology checks; while that lock remains
   held, obtain a lock-free, fault-callback-free journal state hint and inspect
   and recursively validate the live source leaf only when the hint is `absent`
   or `prepared`; perform the authoritative locked journal read, reject any
   state regression, and revalidate the live source only if the authoritative
   state still requires it;
2. call `journal.prepare()` to durably fix the operation binding, storage
   request, checkpoint descriptor, source-owned-root filesystem profile and
   object identity, direct source-leaf filesystem incarnation ID/object ID, and
   predetermined exact result before any physical materialisation begins;
   because journal read/transition callbacks can observe the publication
   namespace, pin candidate/final topology before the call and require the same
   presence and runtime identities afterward before restoring any
   `not-committed` classification;
3. for a fresh or prepared operation, establish the stopped source storage
   barrier by opening and validating the tree, fsyncing regular files, and
   fsyncing directories in post-order so each
   directory is synced only after its descendants; symlink entries are made
   durable by their containing directory; recheck that the source pathname
   still names the caller-observed root inode and that both owned-root
   authorities remain current before and after every injected callback, and
   require that same identity when the copy opens its source root; restore also
   requires the source bundle root to contain exactly `artifact.json` and
   `payload/` at each stable readback; materialized and committed replay instead
   use the recorded source binding and never open, inventory, or inspect the
   caller's current replacement source leaf;
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
   `journal.markMaterialized()` with the fixed digests and filesystem
   incarnation ID/object ID plus a domain-separated digest of every retained
   entry's relative path, kind, and object ID in that incarnation; reject an
   object ID that aliases distinct simultaneously visible runtime objects
   anywhere across journal, source, destination, or retained-tree authorities;
   reject any runtime identity shared with the
   stopped source, and re-open the staging root as current-user-owned and
   extended-ACL-free. Checkpoint bundle envelopes remain mode `0700`; restore
   payload roots retain and pin their modeled portable mode inside the mode
   `0700` destination storage authority. A callback that can observe this
   complete candidate, including callbacks inside the journal's materialized
   transition, is publication-uncertain until a held-lock final-path probe
   proves that the pinned inode was not published. `materialized`
   therefore means a
   complete, durable, unpublished staging object, not merely that copy returned;
7. revalidate the publication root, lock, held staging inode, and absent final
   destination; treat the last pre-rename callback as publication-uncertain
   until a held-lock final-path probe proves that the staged inode is not
   visible there, then repeat the full staged-tree fsync, publication-parent
   sync, exact readback, and pinned identity checks before asking the trusted
   lock holder to atomically rename staging to final with an absent-destination
   precondition;
8. prove that the final pathname names the held staged inode, fsync the held
   publication parent, and perform exact held-inode, payload-digest, and
   `artifact.json` readback; restore readback verifies the payload digest and
   also verifies it against the source bundle manifest and trusted
   `artifactProof`;
9. call `journal.commit()` with the exact materialisation metadata and wait for
   its durable canonical readback and all journal fault/test callbacks to
   return; while the publication lock and held final inode remain pinned,
   repeat final-tree fsync, publication-parent sync, candidate absence,
   authority, identity, and exact digest readback under committed-state error
   classification; and
10. only after the committed record is visible may a consumer replay the
    result or a launcher admit the restore destination.

The source-leaf and destination-root filesystem profiles and persistent object
identities are re-read after every callback and at every materialisation,
rename, and commit boundary. A callback cannot move either path to a different
filesystem incarnation or object generation while preserving a coincidentally
equal runtime device/inode. Inspector failures and mismatches are classified by
the durable state already discovered: committed state is invalid, an outcome
that may already have published remains uncertain, and a proven pre-publication
failure requires recovery without claiming a commit.

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
| `materialized` | absent | absent | The durable stage is missing; trusted recovery is required and prior publication remains uncertain. |
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
if an attacker supplied a staged tree with matching durable identity metadata.

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

A syntactically valid source leaf that is missing, a file, or a symlink is
classified only after the journal is read under the publication lock. It is a
caller error for a new operation and recovery-required for `prepared`, but a
`materialized` or `committed` replay uses the recorded source binding and does
not reopen or recopy that leaf. The binding includes the direct source-leaf
filesystem incarnation ID/object ID: `prepared` replay must still name that exact leaf, while later
states reconstruct the exact binding from the durable record even if the source
leaf is gone or replaced.

A `prepared` replay remains publication-uncertain until candidate and final
probes succeed under the current target authority. Before either probe may
downgrade that uncertainty, the current destination filesystem profile, root
object ID, final name, and derived candidate name must match the durable
publication binding; a caller cannot redirect the probe to a replacement root
or alternate name. A retained candidate is
recovery-required and definitely not committed once final absence is proven. A
visible final remains publication-uncertain because an observable-candidate
callback may have moved the held candidate before `materialized` was recorded.

For a `materialized` replay, physical outcome stays `uncertain` until both
candidate and final probes succeed and prove candidate-present/final-absent
under the current target authority. Retained candidate/final verification also
recomputes the complete path-bound subtree-object-identity digest, rejects any current identity
intersection with an available stopped source, and revalidates owner, pinned
mode, and ACLs. Checkpoint envelopes require `0700`; restore payload roots use
their digest-bound portable mode beneath a private `0700` storage authority.
Same-byte physical-object replacement, inode reuse, or permission broadening therefore cannot advance
to rename or committed replay.

Production root ACL inspection uses the stopped-tree platform defaults. A host
adapter that provides an equivalent trusted ACL capability may inject
`inspectOwnedRootAcl` and `inspectOwnedRootAncestorAcl`; the same inspectors are
used both to pin the publication roots and for cross-root materialisation.

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
