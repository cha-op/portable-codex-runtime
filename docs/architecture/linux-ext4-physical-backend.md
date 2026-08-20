# Linux ext4 Physical Backend

## Scope

The Linux ext4 physical backend supplies concrete, production-injectable
components for the storage, publication, and writer-supervisor seams used by
the PostgreSQL detached-restore deployment. It provisions sparse ext4
filesystem images, creates exclusive directory attachments, durably records
provider operations, publishes stopped trees, and launches writers through
rootless Podman.

Its declared capability boundary is intentionally narrower than automatic
failover:

```js
{
  atomicPointInTimeCheckpoint: false,
  exclusiveWriterAttachment: true,
  fencing: "manual",
  normalDirectoryAttachment: true,
}
```

Only clean, stopped-writer checkpoint publication is supported. A database
epoch, an expired lease, a process exit, or an inaccessible pathname is not a
physical fence. The backend rejects automatic `forceFence()` and raw
`captureCheckpoint()` / `restoreCheckpoint()` calls; the assembled runtime
continues to route checkpoint work through `StoppedDirectoryBackend` and
`StoppedDirectoryPublication`.

## Component Boundary

The physical implementation remains split into small authorities:

- `LinuxExt4Inspector` invokes one fixed absolute native helper. It obtains an
  ext4 filesystem UUID, persistent file-handle identity, and runtime
  `device`/`inode` binding from one pinned file descriptor. It also owns the
  closed FD-operation protocol used by the driver. Helper argv is assembled
  from the frozen request by dense index, and all six runtime `device`/`inode`
  request fields are validated through an indexed frozen key list. Post-import
  Array iterator replacement therefore cannot turn a read-only verb into a
  mutation, retarget a child, or skip canonical decimal runtime-binding
  validation. Persistent filesystem and object identity remain separate
  inspector proofs.
- `LinuxExt4ImageDriver` owns sparse-image creation, `mkfs.ext4`, loop-device
  attachment, ext4 mount inside a host-owned private mount namespace,
  `syncfs`, unmount, loop detach settlement, control-file provisioning, and
  image destruction. Every mutating path is reopened through a pinned parent
  or target authority; ordinary absolute pathnames are never passed to a
  shell.
- `FilesystemImageProviderState` binds one canonical request to one durable
  prepared or committed operation and one storage revision. Its versioned
  checkpoint and bounded per-generation delta log are checked against an
  external monotonic v2 head before every mutation or maintenance rotation.
  Canonical strings are rejected by a conservative UTF-16 code-unit bound
  against the remaining 768 KiB canonical budget before UTF-8 encoding or JSON
  serialization. Before enumerating array own keys, canonicalization also
  requires the array length to fit both the remaining 16,384-node budget and
  the minimum possible bytes left in that 768 KiB envelope. These checks bound
  canonical precursor allocation and resource admission before durable state;
  they are separate from filesystem object identity and access policy.
- `Ext4FilesystemImageBackend` binds the driver and state machine to the raw
  storage lifecycle, restore-attachment, reconciliation, and destination
  resolver contracts. `createInitializedExt4FilesystemImageBackend()` gates
  every serving callback on one cold-open reconciliation attempt.
- `StoppedDirectoryPublication` performs fresh clean-checkpoint publication,
  restore-destination publication, and source-free committed verification.
  The ext4 inspector supplies its atomic filesystem/object observation, while
  provider state supplies the expected destination control identity.
- `PodmanWriterSupervisor` implements the raw version 5 writer-supervisor
  surface with a digest-pinned image reference, rootless execution, a private
  bind, immutable revision publication, stop/join, observer-only launch
  reconciliation, and exact stopped-revision retirement. Every Podman child
  invocation is explicitly local
  through `--remote=false`, and a new launch proves the local rootless ABI with
  the exact `unshare /usr/bin/true` command before it publishes a claim. The
  supervisor also requires matching non-root real and effective user IDs. Its
  Podman child environment adds the fixed `/usr/bin:/bin` search path required
  by reviewed rootless helpers; it never inherits or accepts an ambient
  caller-controlled `PATH`.
- The matching contract version 2 terminal-state collector removes only one
  exact stopped revision 4 chain through the separately settled
  `supervisorStateCollector.collectTerminalState` leaf. It does not share the
  reconciler surface or infer mutation authority from local state.

No one component is a grant authority. PostgreSQL continues to decide whether
one physical mutator may run; these components only validate and execute the
exact invocation they receive.

## Protected Properties

Filesystem observations protect three separate properties. They must not be
collapsed into a generic "stat changed" decision.

### Object identity

An ext4 object is identified by:

- the controlled filesystem UUID, represented as the filesystem incarnation;
- the object-identity scheme;
- a domain-separated digest of the kernel file handle; and
- `device` plus `inode` only as a runtime binding between the pathname, held
  descriptor, and one observation window.

The persistent tuple survives clean unmount, host transfer, and remount.
`device` and `inode` do not. Child-entry churn, directory timestamp changes,
or File Provider-style materialization are therefore not object replacement.
A different filesystem UUID, file-handle identity, or held/path descriptor is
replacement and fails closed.

The ext4 path planner captures every `node:path` helper at module load and
requires each derived image, mount, attachment, restore, and artifact path to
be an exact direct child of its already-canonical parent. Before a new
provision operation is prepared, backend admission also proves that the
storage mount path can host both fixed-shape
`data-<48hex>` and `generation-<48hex>` children within the 4095-byte native
pathname domain. Existing prepared or committed operations and cold-open state
reconstruct their original image and mount plan without retroactively applying
that new admission rule. Module-owned digest components are traversed by dense
index rather than the mutable Array iterator, so post-import prototype
pollution cannot omit components or collapse distinct session and storage
paths. These are lexical nameability and containment prerequisites, not
object-identity or content-change signals.

The inspector rejects duplicate or pairwise-overlapping trusted roots during
construction. A path therefore selects exactly one configured root before any
helper dispatch; a parent plus descendant root cannot turn a durably prepared
operation into a permanently ambiguous retry.

### Content stability

Publication separately binds canonical request/result bytes, journal state,
artifact manifest digest, modeled content digest, and stopped-tree identity
digest. Provider state separately binds canonical operation frames and an
external head covering its generation, checkpoint, and active-log boundaries.
These content signals do not substitute for object identity.

### Access policy

Every owned root and control file is checked for owner, type, link count,
mode, symlink exclusion, ancestor safety, and ACL policy. A persistent object
with a changed access policy is rejected even when its object identity is
unchanged. Failure to read or revalidate policy is distinct from a proved
missing object and remains uncertain.

The exact `inspect-private-path` operation binds that proof to a retained file
descriptor and to the expected device/inode pair. The caller-controlled path
is first pinned with `O_PATH`, then checked for exact identity, type, owner,
mode, and link policy before any read-open can occur. Only a proved regular
file or directory is reopened through its internally generated
`/proc/self/fd/<n>` path; the helper immediately rechecks exact identity and
type on that ACL-capable descriptor. On the ordinary trusted host filesystem,
a substituted FIFO, device, socket, or symlink therefore cannot run an open
hook before rejection. This boundary depends on the deployment's trusted
procfs and host filesystem; it does not claim that an arbitrary malicious
filesystem implements side-effect-free metadata operations. It samples both
`system.posix_acl_access` and
`system.posix_acl_default` before and after reopening the current pathname.
`ENODATA`/`ENOATTR` means that the corresponding POSIX ACL xattr is absent;
`ENOTSUP`/`EOPNOTSUPP` means that this filesystem cannot encode that POSIX ACL
xattr and is also accepted as absent. Any present xattr rejects the private
policy, while every other xattr error is an unreadable policy proof. The
version-1 `commandRunner` and `getfaclExecutable` options remain validated
compatibility placeholders but are neither retained nor invoked as authority.
Directory ctime, size, exact link count, and ordinary child-entry churn are not
access-policy signals.

## FD-Bound Linux Operations

The native helper resolves the selected mount root and target using
`openat2(2)` constraints and returns bounded canonical JSON. It holds the
selected descriptors while validating an operation; for non-lazy unmount it
must release the mounted-root descriptor before dispatch while retaining the
pinned parent/direct-child authority. The driver uses direct Linux syscalls
for mount, unmount, loop configuration, loop inspection, directory creation,
removal, and `syncfs`. The only external storage command is `mkfs.ext4`, which
receives a pinned `/proc/self/fd/<n>` image descriptor and a fixed argument
vector without a shell or ambient `PATH`.

Fresh writable-attachment admission uses the same read-only operation with an
explicit fixed-descriptor emptiness scan. A storage record with no committed
`dataRoot` must pass this empty proof, including replay of a prepared but
uncommitted attach. A stable pre-existing nonempty child is a physical-state
mismatch; if this call created the child, any later proof failure leaves the
physical effect ambiguous. Once `dataRoot` is committed, reattachment is
read-only observation: ordinary committed content growth is allowed, but the
observed root identity must exactly match the durable `dataRoot` identity.
Stable runtime-identity, filesystem, or access-policy mismatch is a physical
state mismatch, and proved absence remains `attachment_root_absent`; an
unreadable or failed inspection remains a physically ambiguous observation.
Emptiness is revalidated after the final stable mount observation for fresh
admission, without using directory size, ctime, or link count as substitutes.
The driver then resamples the persistent ext4 object identity and requires it
to match both the initial inspection and the final empty-policy sample's
runtime identity. This protects the object, policy, and emptiness selected at
the final observation boundary; it is not continuous descriptor custody from
directory creation through the later backend commit. A temporary substitute
that is gone and has restored the original safe object before that boundary is
therefore not itself a mutation signal. Committed read-only observation uses
the same final persistent-identity resampling but deliberately omits the empty
requirement.

Formatting has its own deadline inside the native helper, measured from the
start of the complete format operation. The trusted formatter runs in a new
process group with stdin, stdout, and stderr redirected to a verified
`/dev/null`; only the pinned image descriptor remains available at fd 3.
Before forking it, the helper must successfully make itself a Linux child
subreaper; a missing capability at build time disables this backend, and a
runtime `prctl` failure rejects the operation before formatter dispatch. The
helper allows 40 seconds for ordinary execution, then two seconds after
`SIGTERM`, and finally uses the remaining five seconds to send `SIGKILL`, reap
the retained group leader, reap every adopted child that remains in that PGID,
and require an `ESRCH` process-group absence result. The leader's wait status is
kept separately from discarded descendant statuses. `waitid(..., WNOWAIT)`
keeps the leader PID/PGID reserved until the final deliverable group signal has
been sent; after the leader is reaped, only signal-zero absence probes occur.
A terminal state first observed at or after its applicable phase deadline is
not backdated into an on-time completion.
A parent-death signal plus a parent-PID race check covers the direct formatter
leader when the outer helper is killed before its own deadline. The fixed,
reviewed formatter invocation must not leave a long-lived descendant in that
outer-kill fallback; parent-death signaling is not a process-group broadcast.
The JavaScript 60-second timeout is an independent backstop, not the
formatter-settlement proof.

This boundary protects image-content stability after the helper returns
success: no live formatter process in the supervised PGID can continue
writing the image. It separately protects descriptor-lifetime stability: no
such process can retain fd 3 or a helper pipe after group absence is proved.
Zombie reaping is lifecycle evidence, not either protected property—a zombie
cannot write or retain an open descriptor—but adopting and reaping same-PGID
descendants makes the final group-absence proof independent of PID 1's reap
timing. It assumes the reviewed `mkfs.ext4` and any formatter descendants do
not call `setsid(2)` or move to another process group during ordinary inner
supervision, and do not outlive the leader when the outer timeout wins. An
unreadable clock/wait/group state, `EPERM`, exhausted hard deadline, or a task
that cannot be proved absent remains `operation_outcome_uncertain`; timestamp
or other ordinary `stat` changes are not used as content-mutation evidence.

Mount propagation is a lifecycle property of the deployment process, not a
property that a short-lived helper can create after the fact. The namespace
owner and Node process must remain inside one host-created private mount
namespace for the complete serving window; each helper invocation inherits
that same namespace. Within it, the archive and session mount roots are
separate self-bind mount points marked `rprivate` before any storage-bearing
ext4 mount. Immediately before mount or unmount, the helper binds the parent
carrier by `STATX_MNT_ID` and parses the matching
bounded `/proc/self/mountinfo` record. A `shared:`, `master:`,
`propagate_from:`, or `unbindable` marker is rejected before dispatch; an
unreadable or malformed record is an observation failure. These checks detect
misconfiguration or replacement. They do not replace the host's exclusive
authority over propagation changes by another `CAP_SYS_ADMIN` process in the
same namespace.

Within one process and one loaded driver module, every operation acquires both
its canonical image-path key and mount-path key in a stable global order. This
keeps requests for the same image serialized while also preventing two
different images from concurrently observing one mount point as empty and
creating stacked mounts. The lock covers the complete observation and mutation
sequence and releases both keys on failure. It protects mount-target
exclusivity, not path metadata stability. Separate processes or separately
loaded module copies do not share this queue, so deployment must still enforce
one cooperating mutator for each private mount namespace.

For non-lazy unmount, the mounted-root descriptor remains authoritative
through `syncfs` and a final runtime, persistent-identity, access-policy, and
mount-ID revalidation. The helper then closes that descriptor before calling
`umount2` through `/proc/self/fd/<parent-fd>/<direct-child>`. The pinned parent
remains open throughout. After unmount, the direct child must resolve on the
same parent mount ID and retain its private access policy. This protects the
selected mounted object until the dispatch boundary and the parent/name
location after it; it does not claim continuity for the covered host-directory
inode, which is not part of the current request contract.

Loop detach is not inferred from one successful `LOOP_CLR_FD`. The helper
requires the exact loop `rdev` to report `ENXIO` through
`LOOP_GET_STATUS64`, the block-device `diskseq` to advance, and the matching
sysfs backing-file entry to disappear. The driver then scans all loop mappings
for the exact image object and revalidates the retained image, parent, and
mount authorities. `/run/udev/data` is an asynchronous derived cache and is
not an authority signal for detach settlement. A timeout or unverifiable
kernel boundary is an uncertain outcome.

The trusted native helper path is deployment configuration. Production must
install reviewed bytes at that fixed path as a root-owned, non-writable 0750
file whose dedicated service group contains only the provider identity. It
must not replace the executable while a provider is serving or expose its
capability-bearing interface to other local users.

### Privilege split

The deployment process remains the non-root service user required by rootless
Podman. The default helper runner executes the native helper under that same
real UID; the reviewed helper binary carries only the host capabilities needed
by this implementation (`cap_dac_override,cap_sys_admin=ep`). Keeping the real
UID unchanged is part of the contract because every owned image, mount root,
and control object is created and revalidated against `getuid()`. The helper
itself rejects root execution and any real/effective/saved UID or GID split
before parsing a request. Running the whole deployment through `sudo`, or
installing the helper setuid-root, does not satisfy this boundary.

The Ubuntu conformance jobs install one root-owned, dedicated-group 0750
helper at the fixed path, prove an unrelated user cannot execute it, apply and
read back those exact file capabilities, reject a setuid-root negative copy,
and then run the Node integration as the ordinary service user in that group
inside a long-lived private mount namespace prepared by a root-owned launcher.
A production host may replace file capabilities with an equivalently
constrained `runHelper` broker, but the broker must enter the same namespace,
preserve the caller's ownership identity, and preserve the exact helper
protocol. `CAP_SYS_ADMIN` is a broad host trust root; this design narrows its
executable surface and inputs but does not describe it as a sandbox or a
namespace manager.

## Durable Provider State

Provider-state contract version 2 records exact prepared and committed
operations, complete current storage state, writer authority, mount identity,
data-root identity, and the publication-control tuple. One unresolved prepared
operation blocks a second operation for that storage. A fresh operation also
supplies its complete expected storage state; comparison and append happen
under the same provider lock. A committed delta refers to the prepared-frame
checksum and does not repeat the canonical request; exact replay recovers that
request from the retained prepared operation.

The replaceable provider-state directory cannot authenticate its own deletion,
rollback, or generation substitution. A mandatory `headAnchor` stores the
complete external version 2 head:

```js
{
  contractVersion: 2,
  anchorRevision,
  generation,
  stateRevision,
  baseHeadChecksum,
  checkpointStateRevision,
  checkpointFrameCount,
  checkpointChecksum,
  checkpointBytes,
  frameCount,
  lastChecksum,
  ledgerBytes,
}
```

The head is the rollback authority for the complete provider-state generation:
its monotonic anchor and logical state revisions, generation, previous-head
digest, checkpoint boundary and digest, and bounded active-log boundary and
digest cannot be reset independently. A logical delta increments
`anchorRevision` and `stateRevision`. A pure-maintenance rotation increments
`anchorRevision` and `generation`, retains `stateRevision`, binds the old head
through `baseHeadChecksum`, installs a checkpoint at that exact state revision,
and starts an empty active log. The PostgreSQL adapter stores this head in
migration 8 and advances it with a serializable compare-and-swap. It requires
an otherwise-unused dedicated `PostgresSerializableStore` and therefore a
fifth PostgreSQL pool in addition to the deployment-owned authority,
operation, foreground-lifecycle, and recovery-lifecycle pools. The
provider-state files and their external head must never be restored
independently.

The provider-state directory is a lossless canonical UTF-8 pathname of at most
4,056 bytes, reserving the remaining 39 bytes of Linux's 4,095-byte pathname
domain for `/` plus the longest generation checkpoint name. Every lock,
checkpoint, and log pathname is built through module-load-captured `node:path`
helpers and must still equal one exact direct child of that directory. The
module likewise captures `node:crypto.createHash`, so post-import builtin
replacement can neither redirect durable files nor change SHA-256 frame, head,
or checkpoint identities across restart. Canonical request keys, checkpoint
records, and the retained ancestor-policy chain are also traversed by dense
index rather than the mutable Array iterator. Post-import iterator replacement
therefore cannot change replay identity, checkpoint content, or skip an
access-policy revalidation.

Rotation streams a checksum-framed checkpoint containing every prepared and
committed operation, the exact replay fields for each operation, every current
storage record, and destroyed-storage tombstones. It creates the next
generation's checkpoint and log, syncs both files, revalidates them, and syncs
their parent directory before attempting the pure-maintenance head CAS. Only
the external CAS makes that generation authoritative; an acknowledgement loss
is resolved by exact head readback. After a committed rotation, the previous
generation files can be removed without losing the exact logical history held
by the new checkpoint.

The default soft rotation watermarks are 8 MiB or 8,192 active frames. The hard
per-generation log envelope remains 64 MiB and 65,535 frames. An append that
would cross a soft watermark rotates first, and the hard envelope remains a
fail-closed validation boundary rather than an operator-visible permanent
capacity stop. `inspectCapacity()` exposes the current generation and
revisions, checkpoint bytes and frames, active and remaining log bytes and
frames, configured watermarks, whether rotation is due, retained/prepared
operation counts, and storage count.

The exact-head cache is only a hot-path optimization. The external head plus
the descriptor-bound checkpoint and log content remain authoritative. Stable
metadata can avoid redundant replay only for the exact same head and pinned
objects; metadata change is a content-revalidation trigger, not evidence by
itself of content mutation. Object replacement, changed content, unsafe access
policy, and failed revalidation remain distinct fail-closed outcomes.

Publication-control resolution uses one locked
`readStorageByMountPath({ backendId, mountPath })` query. It scans only storage
records and never projects, clones, or sorts the permanently retained operation
history. No match remains an ordinary absent result; multiple live records for
the same backend and mount path are ambiguous and fail closed. A cold cache may
still replay the authoritative checkpoint and active log before this query, so
the guarantee is independence from operation-history projection on the warm
lookup path, not an end-to-end constant-time open.

This provider-state checkpoint is a control-plane replay snapshot. It is not a
physical ext4 image checkpoint, a published checkpoint artifact, or a content
root. Because exact replay is permanent in version 2, every unique operation
remains in later checkpoints even after it commits. The active log is bounded,
but checkpoint size and aggregate persistent bytes therefore grow with unique
operations.

Migration 010 adds the first retention prerequisite without changing that
production behavior. The permanent
`session_authority.filesystem_image_provider_operations` relation stores each
bounded canonical checkpoint-operation record as exact UTF-8 bytes plus a
domain-separated SHA-256. Explicit metadata binds its kind, storage, logical
revision, prepared checksum, and committed-checksum provenance. Native commits
store `indexed-frame-v1` with the exact checksum. The schema separately
represents a future rotated v2 adoption whose committed checksum is no longer
recoverable; current version 2 reads reject that suffix. Migration 008 already
requires every non-null value in the three head checksum columns to be an exact
64-byte lowercase-hex value. Migration 010 normalizes those valid values to
`varchar(64)` before version 3 and defines the four operation checksum/digest
columns with the same exact format. The new state-authority adapter compares the
complete expected head, advances it, and inserts or
commits the matching operation row in one serializable transaction. A failed
head CAS writes no history; a later record mismatch rolls back the head. The
prepared prefix is immutable, a committed row cannot change again, a row
cannot be deleted while its anchor remains, and `TRUNCATE` is always rejected.
Rotation changes only the head.
Migration 010 permits head contract versions 2 and 3 but does not create a
version 3 writer or alter existing version 2 rows.

Production version 2 still has no retention floor or garbage collection, so a
host must monitor `inspectCapacity()` and the provider-state filesystem. The
next slice must atomically adopt complete version 2 history, make the
PostgreSQL index the exact-replay source, and write version 3 checkpoints that
retain current storage and destroyed tombstones without duplicating permanent
operation history. It must preserve the origin operation for every current
attachment so its committed attachment identity remains reconstructable.

A prepared provision has not exposed a storage result. Its retry may adopt a
currently valid control object on the same deterministic image and mount; the
first committed provider-state CAS starts persistent identity protection.
After that commit, cold open, publication, resolver, quiesce, and destroy all
require the exact recorded tuple.

## Publication Control

Every publication target owns a pre-created
`.stopped-directory-publication.lock`. The expected persistent identity is a
sidecar supplied before queue admission; it is never copied into the caller's
request, coordinator binding, or journal record. While holding the publication
lock, the publisher performs:

1. a held-authority assertion;
2. `lstat` of the real lock path;
3. one atomic filesystem/object inspection bound to that `device`/`inode`;
4. a second held-authority assertion; and
5. exact filesystem-incarnation and persistent-object comparison.

The session-image target obtains its expected tuple from committed provider
state. An archive target needs an equally trusted authority outside the
replaceable archive image. A production composite resolver must reject an
unknown target root; returning `null` is reserved for explicitly selected
legacy deployments and must not silently bypass the ext4 control gate.

The archive image mount root and the artifact-owned child each have a distinct
control inode. Cross-host recovery must preserve and independently authorize
both tuples: the mount-root tuple makes the first remount verification-only,
while the artifact-child tuple authorizes publication into that exact owned
root. Neither identity can substitute for the other.

The operation journal must also remain on storage that preserves its committed
filesystem/object identity. The privileged conformance workflow places the
artifact root and journal root in separate 0700 sibling directories on the
same transferable archive ext4 image.

## Writer Filesystem Authority

The supervisor's default filesystem authority protects the call-time object
selected by the canonical attachment path. It requires a configured attachment
root, proves strict containment, holds both directories by file descriptor,
checks current-user ownership, exact mode `0700`, link/type policy, and the
absence of access or default ACLs. Directory child-entry churn is not an object
or policy change: identity uses the held `dev`/`ino`; current UID, mode, and the
ACL check are access-policy signals. A positive link count independently proves
that the directory remains linked, while its exact value is not compared.
Configured roots, attachment roots, the host Podman executable, holder-frame
paths, and observed mount sources share the native pathname domain: canonical
absolute NUL-free round-trip UTF-8 of at most 4095 bytes. Generic environment,
argv, JSON, and protocol-frame budgets remain separate limits.

Before create, the built-in authority starts a temporary FD holder through
`podman --remote=false unshare` so the holder and later Podman lifecycle
commands join the same rootless user and mount namespaces. The canonical paths
are delivered only in one bounded private-stdin acquisition frame, not in child
argv. The helper opens both directories without following the final component
and returns a bounded fixed-version/status receipt whose identity payload is
limited to PID, descriptor numbers, and decimal `dev`/`ino` fields for the
configured and attachment records. The parent compares that receipt with its
own pinned observations and independently stats the exact
`/proc/<holder-pid>/fd/<attachment-fd>` source.
Each heartbeat repeats the helper-side FD observation and is followed by that
parent-side object and policy proof.

The internally configured container must inspect as non-running external
`created`—Podman 4.9.3's Docker-compatible encoding of
`ContainerStateConfigured`—with one read-write `rprivate` `/session` bind whose
source is exactly that holder FD before `start` is dispatched. A new create
receipt must contain the complete
64-lowercase-hex container ID; a short compatibility identity cannot reach
start. The fixed create shape ignores image-declared volumes, disables
Podman's implicit writable read-only tmpfs mounts, validates the configured
writer executable in the same lossless 4095-byte native pathname domain,
requires every remaining writer argument to be a non-empty, NUL-free,
lossless UTF-8 string of at most 4096 UTF-16 code units. Admitted writer and
Podman environment values use the same NUL-free, lossless UTF-8 code-unit
domain; environment names remain constrained by the fixed ASCII name pattern
and Podman allowlist. Create replaces both image entrypoint and command with
that writer command and selects Podman's `none` log driver. Container output is
not authority evidence;
exact configured/running inspection and the live attachment bracket remain
the success proof, independent of host log-tag or default log-driver policy.
The immutable image's own `Config.User` must be a canonical non-root
numeric `uid:gid`; create maps that pair through
`keep-id:uid=...,gid=...`, so the process can write the current-service-UID
`0700` attachment without broadening its host policy. After start, exact
container ID and PID inspections bracket the live `/session` object and ACL
proof. The holder is released only after that proof or after every process that
may still resolve its FD has crossed the failure barriers below.
Pathname ABA, source substitution, or a bind to another runtime object therefore
cannot produce a started receipt.

The default Podman command runner starts each ordinary CLI in an independent
process group. Timeout, abort, output overflow, or another failure observed
before the direct child's exit latches the first failure and requests whole-
group termination. A natural nonzero result learned after exit does not signal
the frozen numeric PGID because it may already have been reused. Every failure
still remains unsettled until the direct child has closed and a kernel group
probe returns `ESRCH`. It does not treat a partial
`/proc` view or a visible-zombie subset as proof that no hidden live member
exists. The command timeout is therefore a termination-request deadline, not
permission to return while a same-group descendant may still resolve the
holder FD.

The exact local `start <full-container-id>` shape has a stricter mutation
barrier. Podman may already have launched conmon in another process group, so
the runner neither attaches the caller's abort signal nor applies its command
timer after dispatch. A zero exit can continue to exact inspect and live
attachment proof. In the default runner, a post-spawn error, signal, or nonzero
exit remains pending even after same-group cleanup: Podman's internal runtime-
create timeout can return while a separate conmon/crun process is still
resolving the holder source, and this surface has no authenticated conmon PID
or cgroup fence. A Podman or kernel hang therefore keeps the filesystem
authority held instead of returning an error that would release the holder
while escaped runtime work might still use its FD. This is intentionally an
availability non-guarantee. Pre-spawn failure can still return normally because
no Podman process accepted the mutation. The claim is scoped to the reviewed
Podman 4.9.3/conmon/crun execution contract and to a live supervisor/holder;
process crash, host loss, or runtime/configuration drift still requires the
documented external physical fence.

Once exact `start` has crossed that possible-execution boundary, any failure
that reaches the supervisor from later inspect, live-mount, persistent-
authority, durable-transition, or authority-close work is reported only as
`podman_writer_supervisor_outcome_uncertain`; it cannot be downgraded to a
conclusive missing or mismatch result. Likewise, after reconciliation has
observed the exact container running, failure of its subsequent bind or
authority proof is uncertain rather than conclusive. This preserves the
distinction between a proved pre-dispatch rejection and an unverified post-
execution state.

Holder shutdown likewise requires both direct Podman-wrapper close and kernel
`ESRCH` for its detached process group. Forced cleanup signals that group only
before the direct leader's `exit` event proves that its process identity has
ended. After exit, the numeric PGID might have been reused, so cleanup does not
signal it and conservatively waits through direct close for absence instead.
If the operating system cannot prove either group absent, the safe outcome is
to keep the authority held. These barriers protect the selected directory
object's identity and FD lifetime; child-entry or timestamp churn remains
outside the protected property.

The runner receives a closed local-only argv shape: every command begins with
`--remote=false`. This prevents a caller's Podman connection configuration
from redirecting lifecycle authority to a remote service. A new writer launch
does not parse the broad `podman info` inventory as an authority signal. It
instead requires the bounded ABI-only `unshare /usr/bin/true` probe to succeed;
Podman rejects that command in remote or rootful mode. A failed or timed-out
probe is an uncertain observation, while directly observed root real or
effective credentials are a deterministic rootless-policy mismatch.

`createExt4PodmanAttachmentBinding()` now closes the production composition
gap for the initialized ext4 backend. Its persistent authority reconstructs
the complete attachment tuple from the origin operation and current provider
storage record, requires their committed persistent filesystem/file-handle
identity to agree, and asks the driver for a same-sample
`rootRuntimeIdentity`. The Podman composition compares that runtime
`device`/`inode` directly with its held attachment FD and revalidates the live
bind around start. This protects persistent and runtime object identity;
current-user ownership, exact `0700`, link/type, and ACL policy remain an
independent access-policy property. Child-entry, file-content, and timestamp
churn do not replace the directory and are deliberately allowed. The built-in
namespace holder is coupled to the built-in command runner; a deployment that
injects a different runner must also inject its matching trusted filesystem
authority instead of mixing execution domains.

`reconcileWriterLaunch()` has one effectful cold-retirement branch and otherwise
remains a stopped-only observation. Only an exact durable stopped revision 4
record authorizes idempotent `podman rm --ignore` by its bound container ID,
followed by two independent empty `podman ps -a --no-trunc` results filtered by
the exact anchored container name and exact container ID. Only then does its raw
reconciliation receipt, now version 2, carry the revision 4 `terminalRecord`;
the raw launch receipt remains version 2. Ambiguity in `rm`, either absence
proof, physical adaptation, or a pre-commit PostgreSQL finalizer failure
preserves revision 4 and commits no database finalization. A post-COMMIT
acknowledgement loss may instead follow an atomic commit of the operation and
owner-bound GC authorization; exact authorization readback determines whether
that commit exists. Revision 4 remains until the authorized collector removes
it in either case. Observer-only `complete-stopped` and `not-started` receipts
carry a null terminal record and retain the legacy no-GC finalizer. A live or
otherwise ambiguous attempt remains uncertain and blocks successor authority
until an explicit owner stop or physical fence resolves it.

## Terminal Supervisor-State Collection

The owner-private state root still publishes immutable per-attempt revisions
for exact acknowledgement-loss replay, but terminal stopped state now has a
separate bounded collector. `createPodmanWriterSupervisorStateBundle()` keeps
the original state ABI separate from its terminal collector and exposes the
root's persistent high-entropy `stateOwnerId`, whose exact syntax is
`state-owner:<64 lowercase hex>`. First owner preparation creates a unique
sibling staging directory under the held private parent, writes the final-name
canonical marker there, and proves marker identity, exact bytes, `0600` mode,
and single-link policy plus the candidate root's identity, exact namespace,
and `0700` mode. It syncs marker, candidate root, and parent before a same-parent
rename, then revalidates the published root and marker and repeats marker,
final-root, and parent barriers. A pre-rename crash leaves only inert staging
debris, while a retry after rename or acknowledgement loss adopts only the
complete exact marker. Existing malformed or unmarked final roots fail closed
without in-place repair. Node exposes ordinary POSIX `rename()`, not
`renameat2(RENAME_NOREPLACE)`, so an active non-cooperative same-UID process can
still insert an empty final root in the last absence-check window; the protocol
does not claim that stronger property. Exact cleanup of a proven concurrent
loser similarly uses held marker/root descriptors and `nlink == 0` as
post-operation proof because Node exposes no inode-conditioned
`unlink`/`rmdir`. An active same-UID replacement can therefore make cleanup
remove a bait object before the held-object proof fails closed; it cannot make
that failed cleanup authoritative. Later owner preparation reads and
durably validates the published marker; the branded state bundle then supplies
`createPodmanWriterSupervisorBundle()`, which returns the raw supervisor and
collector as one exact process-local branded pair. Production deployment
checks that pair before constructing the physical adapter. Equal
`supervisorId` and `stateOwnerId` strings are necessary but not sufficient, and
direct `createPodmanWriterSupervisor()` validates only a caller-asserted owner
so it cannot satisfy the production deployment boundary. Production does not
derive the owner from the root pathname, `supervisorId`, or `recoveryScopeId`;
missing, malformed, or mismatched markers fail owner preparation or bundle
construction before physical dispatch. This marker is persistent routing
identity, not cryptographic host attestation. An administrator who clones both
the private root and its marker is outside the property it proves. The raw
`supervisorStateCollector.collectTerminalState` physical leaf accepts the exact
immutable stopped revision 4 `terminalRecord` data; a path or attempt ID is not
sufficient input. Its exact version-2 request also carries the bundle's
`stateOwnerId`, and its exact version-2 receipt repeats that owner beside the
attempt ID, status, and terminal-record collection digest. The raw collector
validates the canonical record but cannot
attest its provenance. Production authorization obtains it from the owner
launch, returned stop receipt, or the exact durable revision 4 retirement
receipt. Observer-only reconciliation remains null-record and receives no
collection authority.

An intact first collection validates the complete revision 0 through 4 chain,
every present publication sidecar, the exact terminal record, and the absence of
revisions 5 through 9 before the first unlink. A retry admits only the
oldest-first missing lower prefix produced by phase 1 while revision 4 remains
the exact terminal anchor. Collection then performs two durable phases:

1. Remove revisions 0 through 3 and their sidecars, remove the revision 4
   sidecars while retaining the revision 4 data file as the terminal anchor,
   prove the lower prefix absent, and `fsync` the held state directory.
2. Compare the named revision 4 object and bytes with its already held file
   descriptor, unlink it, then positionally reread its exact canonical bytes
   through that descriptor while revalidating identity and access policy.
   Prove all revision and sidecar names absent and `fsync` the held directory
   again.

On Linux, every artifact lookup, unlink, and final absence proof resolves its
basename through a revalidated clone of the held state-root FD at
`/proc/self/fd/<fd>`. A same-UID replacement of the named root therefore cannot
redirect deletion into a bait directory. Non-Linux builds keep held/named root
identity and access-policy brackets; because Node has no portable
`openat`/`unlinkat`, that fallback does not claim protection from an active
same-UID ABA swap.

A retry may begin from that durable phase-1 prefix. If collection completed but
its acknowledgement was lost, the next exact attempt returns `absent`; the
outer PostgreSQL completion ledger can therefore accept a `collected` to
`absent` replay without restoring deleted local files.

The collector separates three protected properties. Object identity uses
`dev`/`ino` together with the held directory and file descriptors. Content
stability includes the post-unlink held-file positional reread of the exact
canonical bytes. Access policy separately checks same-UID regular/non-symlink
record and sidecar files at exact mode `0600` with required `nlink`, the same-
UID state root and immediate parent at exact mode `0700`, and remaining named
traversal ancestors owned by root/current UID with group/other write permitted
only when sticky. Directory child-entry churn or another generic `stat` delta
is not mutation evidence; it triggers the relevant identity, content, and
policy revalidation. During same-authorization cold overlap, only removal of a
prevalidated record/pending sibling alias is benign: held-FD `nlink` may fall
monotonically within its prior bound, never rise, and every held artifact must
finish at zero links. A pre-mutation I/O or unreadable-state failure, including
a failed pre-mutation object/content/policy revalidation, is
`podman_writer_state_io_failed`; a conflicting canonical chain, supplied
terminal record, sidecar, or future revision is
`podman_writer_state_conflict`; and a failure after any unlink may have begun
becomes `podman_writer_state_collection_outcome_uncertain`, including failure
of the post-unlink held-FD proof. These classes remain distinct from a proved
already-absent result.

The raw collector does not prove that PostgreSQL committed the terminal attempt
or that other callbacks are quiescent. In production, migration 009's owner-
finalizer authorization, the assembled runner's database-global exclusive
restore lifecycle guard, and the collector's independent physical settlement
provide those outer properties.
For this destructive leaf, deadline-plus-grace expiry requests abort and fatal
deployment shutdown but deliberately keeps the invocation and aggregate stop
pending until the raw native Promise settles. The fifth lane therefore keeps
the exclusive lifecycle lease during normal operation while the original
callback can still unlink, and normal pool closure cannot release it early.
PostgreSQL session, connection, or database loss can release the advisory lease
before that callback settles. A later process may then overlap only on the same
immutable authorization; the cold collectors' exact concurrent
idempotent-or-fail-closed protocol preserves state safety without claiming that
the older callback quiesced.
The configured root remains a canonical, lossless UTF-8 native pathname of at
most 4,015 bytes, reserving the remaining 80 bytes of Linux's 4,095-byte domain
for `/<64-hex>.<revision>.json.pending`. Record-key hashing and all path
operations use module-load-captured `node:crypto` and `node:path` intrinsics, so
post-import builtin synchronization cannot redirect or rename durable state.
This is a lexical nameability and persistent-derivation property, independent
of the held-directory identity, content-stability, and access-policy proofs.

## Production Injection

The generic deployment already exposes the required injection points. A host
constructs the inspector, driver, paths, and externally anchored provider
state, then passes them through `createExt4PodmanAttachmentBinding()` so the
initialized backend and Podman filesystem authority cannot diverge. It
constructs publication, prepares the supervisor-state owner, constructs its
branded state bundle, and passes that bundle through
`createPodmanWriterSupervisorBundle()` before creating the deployment. The
deployment rejects independently assembled lookalikes before its physical
adapter is constructed. It maps the accepted pair as follows:

- the physical binding consumes the Podman raw-v5 surface and raw version-2
  reconciliation receipt constructed with the binding's `filesystemAuthority`,
  then exposes the version-4 facade and logical supervisor plus version-2
  logical reconciliation receipt to `runtime.launch.supervisor`;
- `runtime.launch.supervisorStateCollector` receives the matching contract
  version 2 collector with the identical `stateOwnerId`, and deployment gives
  `collectTerminalState()` its own deadline/grace settlement policy;
- `runtime.storage.lifecycleBackend` receives
  `binding.backend.lifecycleBackend`;
- `runtime.storage.publication` receives the stopped-directory publication;
- `runtime.storage.resolveArtifactPaths` and
  `resolveSourceOwnedRoot` receive the ext4 path functions; and
- `runtime.storage.resolveRestoreDestination` receives the initialized
  backend's private resolver with contract version 1.

The host must call `migrate()` on the dedicated provider-state store before
the backend can serve. The archive control identity must come from a trusted
authority outside the archive image. The deployment's four pools must not be
reused as the provider-state pool. Before constructing or initializing the
driver, the host must enter one long-lived private mount namespace, create
separate archive and session self-bind roots there, mark both roots `rprivate`,
and keep the deployment plus every helper or broker invocation in that
namespace until shutdown. Creating the namespace inside one helper call is
invalid because its mounts would disappear when that helper exits.

Rootless Podman's pause process retains the user and mount namespaces created
by the first `podman unshare`. When that creation happens after the ext4 mounts
exist, the pause namespace holds real references to those filesystems even
after the exact writer container is stopped and removed. A deployment that
uses this shape must give the service an exclusive Unix UID and Podman engine,
prove the complete container and pod inventories empty after every Podman
callback is quiescent, and retire the user-wide pause namespace before physical
ext4 quiescence. `podman system migrate` is not a container-scoped operation
and is forbidden for a shared UID or shared engine. A different production
host must instead supply a separately proved scoped namespace-release design.

Shutdown order is also host-owned:

1. stop the generic deployment and wait for admission and settlement drain;
2. after proving an exclusive Podman engine empty, retire its rootless pause
   namespace without issuing another Podman command;
3. enumerate provider state and quiesce every non-destroyed session image;
4. quiesce the archive image and the Podman supervisor state; and
5. close the dedicated provider-state PostgreSQL pool; then
6. drain any namespace-entered helper broker and let the service process exit
   the private mount namespace.

The generic deployment cannot prove that collaborators owned outside its
object graph are quiet.

## Conformance Evidence

The local suites cover exact surfaces, receiver and Promise contracts,
hostile objects, deterministic paths, state restart and rollback detection,
acknowledgement loss, writer-authority transitions, control replacement,
publication atomic observation, Podman replay, and driver request sequencing.

The privileged Ubuntu workflow uses two different hosted runners with the
same explicitly recorded numeric service UID. Each runner starts one
long-lived private mount namespace, prepares separate `rprivate` archive and
session self-bind roots, then drops to the ordinary service user before Node
starts. While both producer images are mounted, a parent/child barrier proves
that the child namespace sees each exact ext4 mount and the parent namespace
does not when the privileged workflow gate runs. After committed attach and
before detach, that same non-root producer Node process uses the initialized
ext4-to-Podman binding to launch a real rootless Podman writer, verifies its
owned `0600` marker, and stops it. Marker polling treats an existing partial
write as transient until the bounded deadline rather than as immediate
corruption. The dedicated hosted runner next proves the complete rootless
container and pod inventories empty and retires the user-wide pause namespace;
no later Podman call can recreate it. The flow then detaches the writer root,
publishes and verifies a fresh checkpoint and restore destination, cleanly
settles both images, and uploads the sparse raw images plus anchored receipts.
The consumer remounts those same
bytes on a new host, verifies the provider head and archive mount-root and
artifact-child control tuples from independent job outputs, rejects a
transferred receipt unless its service UID equals the consumer's, performs
source-free committed checkpoint and restore verification including the
Podman-written marker, reattaches the writer root at a higher epoch, and
destroys both images.

A separate Ubuntu job builds a digest-pinned scratch image and exercises the
rootless Podman launch, bind write, ready proof, stop, and cold reconciliation
surface.

The composed producer proves the ext4 attachment and Podman writer share one
same-process authority boundary. It does not assemble the generic deployment's
independent PostgreSQL gates into one whole-saga run.

These jobs prove clean detach, remount, publication, and cross-host identity.
The namespace retirement only releases the hosted runner's exclusive Podman
engine. Final physical quiescence still comes from the native loop receipt:
unused device identity, advanced disk sequence, and absent sysfs backing.
Container stop/removal alone is not loop-detach evidence.
They do not simulate sudden power loss, storage-controller cache loss,
partitioned stale-writer revocation, or an automatic force fence. Crash-prefix
checkpointing, epoch-enforced fencing, differential export, compression,
encryption, retention, registry signature policy, and remote image transport
remain outside this backend's declared capabilities.
