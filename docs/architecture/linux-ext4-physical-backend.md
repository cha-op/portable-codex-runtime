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
  prepared or committed operation and one storage revision. Its append-only
  ledger is checked against an external monotonic head before every mutation.
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
  launch reconciliation.

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

### Content stability

Publication separately binds canonical request/result bytes, journal state,
artifact manifest digest, modeled content digest, and stopped-tree identity
digest. Provider state separately binds canonical operation frames and an
external ledger head. These content signals do not substitute for object
identity.

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
waits for `LOOP_GET_STATUS64` to report absence, the matching sysfs loop state
to disappear, `udevadm settle` to complete, and a final observation to remain
absent. A timeout or unverifiable boundary is an uncertain outcome.

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

The provider ledger records exact prepared and committed operations, complete
storage state, writer authority, mount identity, data-root identity, and the
publication-control tuple. One unresolved prepared operation blocks a second
operation for that storage. A fresh operation also supplies its complete
expected storage state; comparison and append happen under the same provider
lock.

The ledger file cannot authenticate its own deletion or rollback. A mandatory
`headAnchor` stores this exact external head:

```js
{
  contractVersion: 1,
  sequence,
  lastChecksum,
  ledgerBytes,
}
```

The PostgreSQL adapter stores that head in migration 8 and advances it with a
serializable compare-and-swap. It requires an otherwise-unused dedicated
`PostgresSerializableStore` and therefore a fifth PostgreSQL pool in addition
to the deployment-owned authority, operation, foreground-lifecycle, and
recovery-lifecycle pools. The state ledger and its anchor must never be
restored independently.

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
absence of access or default ACLs, and keeps that same runtime object pinned
through Podman create, start, and the live `/session` observation. Pathname ABA
or a bind to another runtime object fails closed.

The default Podman command runner retains that filesystem authority until the
spawned child emits `close`, which proves both process termination and stdio
drain. Timeout, abort, or output overflow latches the first failure and requests
`SIGKILL`, but it does not settle the runner or release the held directory
descriptor before that close barrier. The command timeout is therefore the
termination-request deadline, not permission to return while a child may still
resolve `/proc/<node-pid>/fd/<fd>`. If the operating system cannot reap the
child, the safe outcome is to keep the authority held rather than return a
normal error that would release it.

That default does not reconstruct the provider state's persistent ext4
`rootIdentity` from an opaque attachment proof. Production must inject the
trusted `filesystemAuthority` seam and bind the complete attachment tuple to
the provider's committed filesystem/file-handle identity before it authorizes
the held object. The current Podman conformance job deliberately exercises the
narrower default authority with a synthetic attachment; the ext4 conformance
jobs exercise the persistent identity independently. They are
production-injectable components, not evidence of that final same-process
identity bridge.

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
