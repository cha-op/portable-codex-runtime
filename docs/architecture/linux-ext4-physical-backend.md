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
  closed FD-operation protocol used by the driver.
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
  serialization, so an oversized request or result cannot force an unbounded
  precursor allocation before durable-state admission rejects it.
- `Ext4FilesystemImageBackend` binds the driver and state machine to the raw
  storage lifecycle, restore-attachment, reconciliation, and destination
  resolver contracts. `createInitializedExt4FilesystemImageBackend()` gates
  every serving callback on one cold-open reconciliation attempt.
- `StoppedDirectoryPublication` performs fresh clean-checkpoint publication,
  restore-destination publication, and source-free committed verification.
  The ext4 inspector supplies its atomic filesystem/object observation, while
  provider state supplies the expected destination control identity.
- `PodmanWriterSupervisor` implements the raw version 2 writer-supervisor
  surface with a digest-pinned image reference, rootless execution, a private
  bind, immutable revision publication, stop/join, and stopped-only read-only
  launch reconciliation. Every Podman child invocation is explicitly local
  through `--remote=false`, and a new launch proves the local rootless ABI with
  the exact `unshare /usr/bin/true` command before it publishes a claim. The
  supervisor also requires matching non-root real and effective user IDs. Its
  Podman child environment adds the fixed `/usr/bin:/bin` search path required
  by reviewed rootless helpers; it never inherits or accepts an ambient
  caller-controlled `PATH`.

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
that new admission rule. These are lexical nameability and containment
prerequisites, not object-identity or content-change signals.

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
root. Because exact replay is permanent in this version, every unique
operation remains in later checkpoints even after it commits. The active log
is bounded, but checkpoint size and aggregate persistent bytes therefore grow
with unique operations. This slice has no retention floor or garbage
collection; a production host must monitor `inspectCapacity()` and the
provider-state filesystem. Future work may define an authority-safe retention
floor or move exact replay history to an indexed PostgreSQL representation.

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
replaces both image entrypoint and command with that writer command, and
selects Podman's `none` log driver. Container output is not authority evidence;
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
attachment proof. Any post-spawn error, signal, or nonzero exit remains pending
even after same-group cleanup: Podman's internal runtime-create timeout can
return while a
separate conmon/crun process is still resolving the holder source, and this
surface has no authenticated conmon PID or cgroup fence. A Podman or kernel
hang therefore keeps the filesystem authority held instead of returning an
error that would release the holder while escaped runtime work might still use
its FD. This is intentionally an availability non-guarantee. Pre-spawn failure
can still return normally because no Podman process accepted the mutation. The
claim is scoped to the reviewed Podman 4.9.3/conmon/crun execution contract and
to a live supervisor/holder; process crash, host loss, or runtime/configuration
drift still requires the documented external physical fence.

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

That default does not reconstruct the provider state's persistent ext4
`rootIdentity` from an opaque attachment proof. Production must inject the
trusted `filesystemAuthority` seam and bind the complete attachment tuple to
the provider's committed filesystem/file-handle identity before it authorizes
the held object. The current Podman conformance job deliberately exercises the
narrower default authority with a synthetic attachment; the ext4 conformance
jobs exercise the persistent identity independently. They are
production-injectable components, not evidence of that final same-process
identity bridge. The built-in namespace holder is coupled to the built-in
command runner; a deployment that injects a different runner must also inject
its matching trusted filesystem authority instead of mixing execution domains.

`reconcileWriterLaunch()` is a repeatable stopped-only observation. It may
enumerate and inspect the exact container and prove the live bind identity, but
it cannot adopt a new started writer, stop or remove a container, or mutate the
private supervisor journal. A live or configured ambiguous attempt therefore
remains uncertain and blocks successor authority until an explicit owner stop
or physical fence resolves it. Container retirement occurs only after the
grant-bearing returned stop callback has durably reached its private stopped
record.

The supervisor state currently retains immutable per-attempt revisions for
exact acknowledgement-loss replay and has no authority-owned compactor. A
production host must place that owner-private root on monitored dedicated
storage. Bounded retention or garbage collection requires a separate callback
after PostgreSQL has permanently committed the exact terminal attempt and all
callbacks for that attempt are quiescent; the read-only reconciler cannot be
used as that authority.

## Production Injection

The generic deployment already exposes the required injection points. A host
constructs the inspector, driver, paths, externally anchored provider state,
raw backend, initialized backend, publication, and supervisor before creating
the deployment. It maps them as follows:

- `runtime.launch.supervisor` receives the Podman v2 surface;
- `runtime.storage.lifecycleBackend` receives
  `initializedBackend.lifecycleBackend`;
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

Shutdown order is also host-owned:

1. stop the generic deployment and wait for admission and settlement drain;
2. enumerate provider state and quiesce every non-destroyed session image;
3. quiesce the archive image and the Podman supervisor state; and
4. close the dedicated provider-state PostgreSQL pool; then
5. drain any namespace-entered helper broker and let the service process exit
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
does not when the privileged workflow gate runs. The producer publishes a
fresh checkpoint and restore destination,
verifies both, cleanly unmounts and detaches both loop devices, and uploads the
sparse raw images plus anchored receipts. The consumer remounts those same
bytes on a new host, verifies the provider head and archive mount-root and
artifact-child control tuples from independent job outputs, rejects a
transferred receipt unless its service UID equals the consumer's, performs
source-free committed checkpoint and restore verification, reattaches the
writer root at a higher epoch, and destroys both images.

A separate Ubuntu job builds a digest-pinned scratch image and exercises the
rootless Podman launch, bind write, ready proof, stop, and cold reconciliation
surface.

The ext4 and Podman jobs prove privilege-compatible components, not one
same-process end-to-end deployment. A production composition must still bind
the initialized ext4 attachment's persistent identity to the Podman filesystem
authority in the same non-root process and satisfy the generic deployment's
independent PostgreSQL gates.

These jobs prove clean detach, remount, publication, and cross-host identity.
They do not simulate sudden power loss, storage-controller cache loss,
partitioned stale-writer revocation, or an automatic force fence. Crash-prefix
checkpointing, epoch-enforced fencing, differential export, compression,
encryption, retention, registry signature policy, and remote image transport
remain outside this backend's declared capabilities.
