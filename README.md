# Portable Codex Runtime

Portable Codex Runtime is an experimental host runtime for moving Codex
app-server sessions between trusted machines while keeping the execution
environment, workspace, rollout state, and recovery data explicit.

The current repository combines compatibility probes for authentication and
interrupted-turn recovery with an offline, pinned-runtime rollout-tail repair
primitive, the storage contracts, journal, local stopped-directory
publication, same-process stopped-writer authority, and a composed
stopped-directory backend for guarded clean capture, committed-result
reconciliation, durable stopped-writer-to-prepared-capture handoff, and
restore.
It also includes source-free committed restore-destination verification, a
versioned provider attachment proof, atomic detached activation into a
prepared launch, four bounded no-relaunch recovery lanes, and a
production-neutral detached-restore foreground composition seam and runtime
assembly with a narrow same-launcher writer-start ingress.
The planned runtime keeps refresh tokens in a central auth authority, injects
short-lived access tokens into session workers, and treats session data
snapshots separately from monotonic credential state.

## Status

The runtime architecture is under active development. The current implementation
proves that the installed Codex app-server supports external ChatGPT access-token
injection and proves the managed refresh API choreography with an explicitly
uncontained host probe. Production managed refresh fails closed until a
per-refresh rootless containment executor is implemented. A separate loopback
probe characterizes explicit interruption, process signals, hard kills, a
stopped-tree restore, and both supported rollout-tail repairs without using
credentials or a real model turn. The repair compatibility evidence binds one
private Codex executable by version and SHA-256; it does not claim OCI
same-image recovery.

The `chatgptAuthTokens` protocol is an experimental Codex app-server API. Pin the
Codex binary or image digest and rerun these probes before upgrading it.

## Auth Broker MVP

The runtime now includes an encrypted canonical auth-state store and a broker
state machine. AES-256-GCM envelopes bind authority, key ID, monotonic uint64
generation, base generation, commit ID, operation, and payload. Compare-and-
swap publication uses a private directory, advisory lock, same-directory
rename, directory sync, canonical readback, and exact idempotent replay after a
lost acknowledgement.

The broker structurally cross-checks ChatGPT auth mode plus decoded access/ID
JWT identity, plan, and expiration claims; coalesces compatible refreshes;
publishes only an exactly re-read committed generation; and durably blocks
reauthentication or uncertain post-dispatch states. Before OAuth dispatch it
replaces `ready` state with a credential-free durable recovery reservation.
Its unique owner ID prevents ABA dispatch races, while a one-way source-token
digest distinguishes a completed concurrent refresh from a safe restore. A
crash or failed outcome commit therefore cannot expose or reuse the consumed
old refresh token. Canonical realpath identity collapses local filesystem
aliases, and key rotation can advance the envelope generation without
discarding an already-produced outcome. A per-worker facade privately
remembers the last delivered generation and access token, so a stale `401` receives a genuinely
newer credential while a same-token state change does not suppress a required
refresh. Workers receive only the access token, account ID, and plan type
through the pinned experimental app-server protocol.
Ordinary login cannot overwrite an active refresh reservation; explicit crash
recovery requires its exact generation and owner ID after the supervisor has
fenced the old broker process.
See `docs/architecture/auth-broker.md` for the security and recovery contract.

## Session Storage Contracts

The runtime now has executable v1 record validators for a secret-free session
manifest, trusted OCI-resolution matching, uint64 fencing epochs,
lease/attachment matching, dedicated exact force-fence request/result
envelopes, declared storage backend capabilities, structural rootless worker
directory binds, and recovery checkpoint classes. Physical launch, fencing,
and snapshot authorization remain the responsibility of later concrete
adapters and their conformance tests.
An optional version 1 restore-attachment activation extension binds the exact
committed publication object and materialization digest to the provider's
attach mutation, canonical attachment, and proof. Path equality remains
correlation evidence rather than object or attachment authority.
The worker sees one ordinary directory at `/session`; a host storage agent owns
raw volumes, filesystem images, attach/mount operations, and stale-writer
fencing. `CODEX_HOME`, the effective Codex `sqlite_home`, and the workspace
remain on that single-attached session volume. The launcher fixes
`sqlite_home` through a Codex CLI config override and rejects request-level
changes; auth authority and canonical lease state remain outside the volume.

The default session policy permits 6 subagents, can be raised to a hard limit
of 10, and permits nesting through depth 2. Git Summary remains deferred and is
not part of checkpoint correctness. See
`docs/architecture/session-storage-contracts.md` for the state machine,
backend interface, NFS/image constraints, and Codex source basis.

## Session Authority Foundation

The control-plane foundation supplies a single-client PostgreSQL
`SERIALIZABLE` transaction executor and an ordered, checksum-bound migration
chain for canonical sessions, operation and reservation claims, capture
tombstones, the checkpoint catalogue, and restore-generation identities, plus
real-PostgreSQL concurrency coverage. The installed migration ledger must be
an exact contiguous prefix; gaps, future rows, malformed rows, or checksum
drift fail closed. The
canonical registry and durable operation kernel now bind immutable session
identity, one exact session-wide mutation reservation, a single definite
dispatch claim, retained uncertainty, and safe pre-dispatch cancellation. A
versioned terminal anchor ties every progressed inactive session to the latest
committed operation and released reservation; legacy version 1 and version 2
snapshots keep their exact request identity until the next state write upgrades
them to canonical document version 3.
Typed writer acquisition now allocates one database-clock lease and uint64
epoch, finalizes exact provider mutation and attachment evidence from definite
or uncertain dispatch, and renews only the lease expiration through an
idempotent terminal operation. `writer-release-v1` preserves the exact owner
tuple and epoch while dispatching detach, including unchanged-owner cleanup
after expiry. `writer-force-fence-v1` advances the epoch only at its definite
typed dispatch commit from `ATTACHED` or `BLOCKED`; only the matching dedicated
provider proof can finalize `FENCING -> DETACHED`. Ambiguous or unavailable
provider outcomes finalize to `BLOCKED` with the tuple, target, and current
epoch retained. These paths use generic reserve, typed dispatch, provider
execution outside the transaction, and typed finalization without schema DDL.
Unit and real-PostgreSQL coverage exercise exact replay, expiry, ambiguity,
manual-backend rejection, epoch retention, and explicit
`BLOCKED -> FENCING` recovery.

A provider-neutral PostgreSQL writer-detach composition now owns the complete
live release or force-fence invocation. It accepts one branded per-operation
advisory guard across reserve, typed dispatch, provider execution, and
exact-proof finalization; validates every authority receipt as one canonical
operation/reservation/session transition; validates backend identity and
capabilities before durable work; and invokes a provider only after a definite
dispatch grant. A retained
`starting` or `uncertain` operation never authorizes provider replay. Because
storage contract v1 has no provider-outcome query, that state is finalized as
`BLOCKED`; by contrast, a valid proof followed by database finalization
acknowledgement loss replays only the same finalizer. Manual fencing records
`fence-unavailable` without calling the provider. This facade is not yet wired
into the production checkpoint adapter and does not enable `runRestore()`.

The original production clean-capture authority reuses the operation,
reservation, attempt, tombstone, and catalogue schema. It binds one
exact session-wide operation and reservation to one globally unique
capture-attempt claim, holds a per-operation PostgreSQL session advisory guard
while local publication runs outside database transactions, and atomically
finalizes the matching checkpoint catalogue entry, operation, reservation,
and terminal anchor. Attempt and operation claims are retained permanently; a
pre-existing tombstone is non-authorizing and always rejects reuse. A
source-free reconciliation path may verify only the exact already committed
journal record and artefact for that durable attempt, including after lease
expiry or fence turnover. It cannot consume another stopped-writer capability
or advance `prepared` or `materialized` publication.

Migration `006-writer-stop-capture-handoff.sql` adds the durable V3 bridge from
writer stop to that capture authority. A version 3 `writer-launch-stop-v1`
request embeds the complete canonical capture intent, including its fixed
capture operation and attempt identities and predetermined result. Its typed
stop dispatch transaction permanently preclaims the capture operation ID
before physical stop can begin. After exact `complete-stopped` evidence, one
`SERIALIZABLE` finalizer commits the stop, materializes that same claim,
creates the exact `checkpoint-capture-v1` operation and reservation as
`prepared`, and makes it the session's active operation. Rollback exposes none
of the handoff writes.

`PostgresSessionAuthority.listCheckpointCaptureRecoveryCandidates()` adds a
bounded read-only recovery page over retained `starting` or `uncertain`
captures and V3 handoff captures that are still exactly `prepared`. It uses
immutable `session_id` keyset order, the existing active-row index, a hard
`limit + 1` query, and one serializable snapshot to cross-check the session
pointer, reservation, attempt or materialized handoff binding, tombstone
absence, and catalogue absence. Each frozen candidate contains only the exact
durable `{checkpoint, request}` admission plus its state; current writer
authority is never reconstructed.

`createPostgresCheckpointRecoveryService()` exposes
`runBatch({afterSessionId, limit, signal})`, which processes one bounded page
sequentially against one backend and stable artefact-root configuration.
One service instance admits at most one batch at a time; an overlapping valid
call fails closed before it can enumerate or reconcile another candidate.
An optional prepared-capture callback routes only an exact `prepared`
candidate through its one `prepared -> starting` dispatch grant and the
backend's fresh-only `resumePreparedCheckpointCapture()` entry point.
`starting` and `uncertain` candidates stay source-free and may only verify an
already committed publication. Once the prepared dispatch grant may have
committed, ambiguity never authorizes a second publication. Per-candidate
receipts are `reconciled` or `pending`; the batch is
`sweep-complete`, `limit-reached`, or `aborted`, and the cursor advances only
after the current attempt settles. A completed sweep wraps to a null cursor for
later replay. Abort signals stop only new admission and drain any in-flight
guarded reconciliation without `Promise.race`; guard-busy or unverifiable work
remains durably blocked, and an unresolved `starting` operation may advance to
`uncertain`. The count bound does not provide a wall-clock bound because the
committed verifier has no cooperative cancellation seam, so deployment still
needs statement and request deadlines.

This production adapter is capture-only. The PostgreSQL authority now exposes
typed restore-generation reservation, single-dispatch claim, finalisation,
exact replay, read, and bounded recovery transitions. The durable request
retains the backend's exact `{checkpoint, request}` admission; the claim binds
one fresh generation identity and destination-isolation proof identity to the
complete expected session, committed source catalogue, current destination
lease, attachment, storage reference, and strictly newer restore fence. That
proof identity must come from the trusted destination authority in later
composition; the serialized ID is not self-authenticating.
`authorized` and `committed` generation rows remain permanent, and a claim
whose commit acknowledgement is lost never grants a second publication
dispatch. Publication still runs outside the database transaction, and a
committed generation is durable input to the later atomic handoff or detached
activation; it is not attachment or launch authority by itself.
The same operation and reservation schema now also carries typed durable
writer-launch attempts. One attempt binds an exact committed generation,
attachment, lease and fence tuple, bounded measured-image projection, and
trusted supervisor identity before granting a single dispatch. Claim validates
the committed generation's session-history anchor and acquires its relation
locks before the final database-clock lease check. `starting` and `uncertain`
attempts remain durable blockers. Exact supervisor evidence can finalize an
attempt as started, not started, or completely stopped; a started result is
anchored by canonical session document version 3 even after later terminal
operations replace `lastOperation`. No launch-specific table is required, and
the serialized generation, measurement, process, writer, and proof identifiers
are correlation values rather than launch capabilities.

A hardened PostgreSQL logical-writer-launcher facade now composes the next
same-process boundary. It revalidates the original image reservation, reserves
the exact attempt and advances it durably to `starting`, consumes that one-use
capability, and compares its measured result before invoking one external
launch callback. A started writer is registered provisionally with the
stopped-writer coordinator before durable started finalisation and becomes
resolvable only after exact readback. Prepared recovery cancels without launch.
For `starting` or `uncertain`, an exact same-process provisional record first
retries started finalisation with its original evidence; otherwise recovery
uses stopped-only supervisor reconciliation and never relaunches. An already
committed started attempt can be adopted only from that exact local record,
otherwise stop or physical fencing is required.

Restore-generation request version 2 now closes the database crash gap between
generation finalisation and launch reservation. It binds one exact durable
launch intent before publication. A single serializable transition commits the
generation and restore terminal anchor, reserves the matching existing
`writer-launch-attempt-v1`, and advances the session to that prepared launch.
The launcher can first prepare the intent from the original opaque image
reservation and later claim only that pre-reserved attempt. It consumes the
reservation and invokes the supervisor only after durable `starting`; a
mismatch leaves the attempt untouched. If the process restarts while the
attempt is still durably `prepared`, the runtime may mint a fresh opaque
reservation for the same fixed image and measurement; once the attempt may
have reached `starting`, recovery never consumes another image capability or
relaunches.

The authority also provides typed `writer-launch-stop-v1` transitions that
preserve the original started attempt and clear the current launch only after
exact `complete-stopped` supervisor proof, plus bounded discovery of prepared
or active attempts and current launches. Stop request contract version 2 adds
a persisted claimant digest; a local raw-token plus attempted-claim witness
recovers a committed stop claim whose acknowledgement was lost without
authorizing foreign or never-attempted `starting` state. Exact legacy request
version 1 remains readable and finalizable with its original edge-only claim
semantics. The launcher now routes one
same-process coordinator stop through that durable transition and composes one
clean capture. Its locked stop reconciliation preserves an ambiguously
committed operation identity while allowing a strictly newer, validated lease
snapshot to replace a pre-reserve input only after the authority proves the
operation absent.

Stop request contract version 3 extends that same operation kind with the
exact capture intent. Fresh V3 reservation is default-closed unless startup
sets `writerLaunchStopV3FleetCompatible: true`; exact replay and recovery of
already durable V3 work remain available when the creation gate is closed.
Legacy V1/V2 stop and same-process capability capture retain their existing
contracts. The new prepared path revokes the transient stop capability after
the atomic handoff, then lets the stopped-directory backend claim only the
pre-created capture and publish fresh once. The launcher's local writer
exclusion is retired only after the returned committed capture result exactly
matches the predetermined result embedded in the V3 stop request.

Detached restore activation now composes the next authority boundary after the
predecessor is stopped, cleanly captured, fenced, and canonically detached.
`verifyCommittedRestoreDestination()` is source-free and read-only: it
revalidates only the exact committed journal record, trusted capture proof,
publication identity, final filesystem object, modeled tree, and access-policy
binding. It never reads the old capture source or advances an absent,
`prepared`, or `materialized` publication. The optional provider version 1
extension then returns attachment and mutation proof for that same published
object. Typed `restore-attachment-activation-v1` finalization serializably
installs the exact canonical attachment and materializes its predeclared
`writer-launch-attempt-v1` as `prepared` in one atomic transition. Exact
readback resolves commit acknowledgement loss without a second provider
activation.

The logical launcher can now prepare its durable intent from an exact clean
version 3 `DETACHED` session whose committed terminal operation is release or
force-fence. Preparation revalidates but neither consumes the opaque image
reservation nor creates database state. After activation atomically installs
the attachment and materializes the prepared attempt, `runPreparedLaunch()`
validates that activation-produced handoff, claims the existing attempt, and
reuses the same no-reserve, no-relaunch replay rules.

Activation request version 1 retains its historical direct stop-to-detach and
same-generation predecessor relation for exact replay. Request version 2 adds
the missing production relation: the current writer's committed stop must feed
one committed clean checkpoint capture, and only then may release or
force-fence detach that same old attachment. The target restore generation is
bound to the old attachment but may differ from the stopped writer's launch
generation. Its historical durable topology reads backward through detach,
capture, and stop. That topology and exact replay remain compatible. A fresh
generation-predecessor topology may instead commit the version 1 target
generation after capture and before detach, so its backward chain is detach,
generation, capture, and stop. Independent default-deny authority options gate
fresh restore-generation-v2, activation-v2, and this new activation-v2 topology
only after exact durable lookup: `restoreGenerationV2FleetCompatible`,
`restoreAttachmentActivationV2FleetCompatible`, and
`restoreAttachmentActivationV2GenerationPredecessorFleetCompatible`. Closing
any creation gate does not disable exact replay or recovery of existing work.

The bounded restore recovery service sweeps four independent keyset lanes:
destination generations, attachment activations, prepared or active launch
attempts, and current-launch inventory. Recovery may verify committed
publication, replay an idempotent provider activation, finalize durable state,
or reconcile stopped-only supervisor evidence. It never republishes, reserves
or consumes an image, invokes the launch callback, reconstructs an opaque
writer capability, or treats current-launch inventory as adoptable work.

A database-global restore lifecycle guard now uses one versioned PostgreSQL
session advisory-lock identity for the complete authority candidate universe.
Foreground composition can hold a shared lease, while each bounded recovery
pass holds the matching exclusive lease. The recovery runner revalidates that
lease around lane reads, reconciliation batches, and durable cursor advances;
the service revalidates it around listing and each admitted candidate. The
fixed lifecycle lock uses a versioned advisory-key namespace distinct from
ordinary durable operation IDs, so an operation whose ID matches the lifecycle
label cannot self-conflict with the outer shared or exclusive lease. Foreground
shared admission and recovery-exclusive admission use distinct dedicated
operation-guard pools, so exhausting foreground connections cannot delay the
recovery lock attempt or scheduler shutdown. The detached-restore foreground
factory also rejects an inner per-operation guard that reuses either lifecycle
pool, preventing a max-one pool from self-deadlocking while the outer shared
lease waits for a nested exclusive operation. The
underlying operation guard uses callback-only node-postgres adapters and a
callback-scoped `complete(value)` carrier, so driver results and asynchronous
lifecycle results cannot be assimilated through mutable Promise or object
prototypes before the advisory-lock probes and cleanup drain. The
production recovery scheduler runs one immediate bounded pass, then fixed-
delay non-overlapping passes, coalesces concurrent kicks, and drains an
admitted pass before shutdown settles. Its `onStep` hook is synchronous
notification only and must return exactly `undefined`; Promise, thenable,
generator, and other values fail the scheduler closed. A returned safe native
Promise is rejection-drained but never awaited. Cursor recovery scopes remain
fairness and replay identities only; they do not partition the lifecycle lock.

Detached-restore foreground phase A now provides one caller-persisted stable
root plan and a production-neutral composition facade. The plan binds the
outer restore request, source checkpoint artefact and detached destination,
stable capture timestamp, detach mode, holder, image plan, and lease duration;
domain-separated hashes derive the renewal, safety-capture, generation,
detach, activation, and launch identities. The logical launcher and capture
authority still mint the formal stop-operation and capture-attempt identities.
The source checkpoint paths are not the fresh safety-capture paths: the
capture backend resolves the latter from the derived capture identities.

Migration 7 adds an immutable PostgreSQL stable-plan registry. Provisioning is
a separately gated insert-or-exact-replay operation that permanently claims
the restore operation ID in the shared operation-ID namespace before storing
the canonical admission and plan inputs. A crossed session, request, or plan
identity fails closed; a lost commit acknowledgement is reported as uncertain
until exact durable readback proves the inserted plan, in which case the
provisioning call returns that reconstructed plan. Resolution accepts the
expected canonical session, performs read-only exact-identity verification,
and rehydrates the authentic in-process plan capability without creating or
repairing a row. When the reserved restore operation reaches generation
dispatch, the authority revalidates that complete rehydrated plan against the
permanent claim's `planSha256` and requires its generation and destination-
isolation identities to match the typed claim before publication authority can
be granted.

Each facade invocation first distinguishes fresh work from an exact typed
durable continuation. Fresh work must pass the default-deny detached-production
fleet gate; an already-materialized V3 stop-to-capture handoff or later typed
operation may continue without reopening fresh admission. A stop that may have
started before that atomic handoff remains blocked. One shared restore-
lifecycle lease spans:
lease renewal before stop, V3 stop-to-prepared-capture, version 1 target-
generation publication, canonical release or force-fence detach without
fallback, activation version 2, and prepared launch. Exact retries reuse the
same persisted plan and durable subordinate identities; the standalone facade
accepts that resolver as a collaborator, while the assembled runtime binds its
private resolver to the PostgreSQL registry. There is no autonomous
cross-stage saga that guesses progress after restart.

The configured lease duration must cover both database-clock claim windows:
the safety-capture-to-generation boundary and the activation-to-launch
boundary. Expiry fails closed and never authorizes a second physical dispatch.

Production restore nevertheless remains fail-closed. The production
checkpoint adapter's `runRestore()` stub is unchanged. Remaining phase-B work
must supply provider/image, PostgreSQL connection/bootstrap configuration, and
operational lease-budget bindings plus the final public backend, then add end-
to-end ambiguous-outcome coverage before enabling that entry point. A
production-neutral runtime factory now constructs the capture-only backend,
standalone foreground facade, idle scheduler, and a narrow `writerLaunch`
facet plus a narrow `stablePlanProvisioning` facet from one internally
consistent graph and four caller-owned pool objects that must be pairwise
distinct. The launch facet exposes only `runLaunch()` and
`reconcileLaunchAttempt()` from the same process-local logical launcher used by
the backend and foreground composition. The provisioning facet exposes only
`provisionStablePlan()`; the same internally constructed registry's
receiver-preserving read-only resolver is private to foreground execution.
This makes the original opaque writer handle reachable by later stop/capture
without making a committed database row or another launcher authoritative.
The low-level factory does not migrate, start, stop, or close pools, replace
the capture-only backend's fixed restore route, or wire foreground restore
into the production adapter. A deployment controller now invokes its narrow
same-store migration facet, requires the scheduler's immediate pass to prove a
complete recovery sweep, and then opens only gated foreground, plan-
provisioning, and writer-launch facets. Stop closes those facets, stops the
scheduler, and drains admitted calls; the caller closes the four borrowed pools
after that barrier. The exclusive scheduler continues bounded no-relaunch
recovery.
Crash-consistent ext4 or filesystem-image backend execution, differential
compression, periodic backup, and cross-host restore verification remain later
work; neither a database lease nor a higher epoch is a physical writer fence.

A bounded runnable-image profile binds exact OCI/Docker platform-manifest and
config bytes, validated layer descriptors and rootfs DiffIDs, the Linux
platform, and normalized Codex version to a one-use in-process object
capability. This closes serialized image-identity substitution inside the
authority boundary. It intentionally rejects artifact manifests and unsupported
descriptor extensions; it does not implement registry signature policy, mount
an image, launch Podman/Docker, or turn a database epoch into a physical
stale-writer fence. The stopped-directory backend remains manual-fencing only.
See `docs/architecture/session-runtime-authority.md`.

## Snapshot and Restore Core

The backend-neutral core orchestrates stopped-writer `clean` checkpoint
capture and restore. It validates the manifest, storage, attachment, lease,
and operation request, returns a portable descriptor only after a definite
backend result, requires a restore epoch greater than the source epoch, and
fails closed on every uncertain post-dispatch outcome. Atomic fence rechecks,
storage barriers, durable idempotency, and physical capture or restore remain
backend responsibilities. See `docs/architecture/snapshot-restore-core.md`.

An optional versioned backend extension also lets the core reconcile the exact
original clean-capture request after lease expiry or fence turnover. That path
has no writer, lease, attachment, clock, or stopped-writer capability input: it
can only validate a result already committed by authenticated durable attempt
state and the physical backend.

## Same-Process Stopped-Writer Capability

The runtime can convert one trusted, fully joined writer stop into one
same-process object capability for one snapshot callback. Private object
identity binds the capability to the exact process and writer incarnations,
complete attachment, writer fence, and stop operation. The original reference
may be delegated inside the issuing process, but serialization or
identity-breaking clones produce inert lookalikes and cannot transfer authority
to another host. Stop or snapshot uncertainty is terminal for that writer and
capability, which are never reused to re-dispatch the callback.

Codex lifecycle events such as `turn/completed`, `ShutdownComplete`, and
`thread/closed` are observations rather than writer-stop proof. Production use
requires a supervisor that joins the complete container, cgroup, or VM writer
boundary, or a future Codex shutdown path that joins every persistence writer.
Canonical fence rechecks, durable idempotency, and physical publication remain
backend responsibilities. See
`docs/architecture/stopped-writer-capability.md`.

## Reusable Stopped-Tree Primitives

The stopped-tree validation, copy, digest, and guarded-cleanup logic is now a
reusable module rather than probe-owned code. It preserves the probe's strict
owned-root, mount, pathname, symlink, metadata, and identity-race rules. This
layer still has no fsync barrier, atomic publication, durable operation journal,
descriptor replay, or storage backend. See
`docs/architecture/stopped-tree-primitives.md`.

## Durable Filesystem Operation Journal

The host-local journal durably records exact storage operation state through
`prepared`, `materialized`, and `committed` phases. Canonical copy-on-write
records use file fsync, held-lock rename, parent-directory fsync, and exact
readback; committed results can be replayed after restart. The journal records
caller-supplied state but does not prove physical materialisation, writer stop,
fence authority, atomic publication, destination isolation, NFS guarantees, or
backend success. Its fresh-prepare operation atomically rejects every existing
phase when a higher layer must prove that an operation started inside the
current authority transaction. See
`docs/architecture/filesystem-operation-journal.md`.

## Stopped-Directory Publication

The local stopped-directory publication layer binds that journal to physical
storage work. It holds one publication-root lock, prepares the exact journal
record, establishes a post-order source fsync barrier, builds and fsyncs a
deterministic private stage, records the exact digest and held identity as
`materialized`, atomically renames only onto an absent final destination,
fsyncs and reads back the final object, and only then commits the journal.

Checkpoint artefacts are self-describing `artifact.json` plus `payload/`
bundles. Restore validates the bundle against a trusted capture proof from the
committed catalogue, then publishes only the payload tree. A visible final path
remains unusable by consumers and launchers until its exact journal record is
committed. Partial stages and uncertain final objects are retained as recovery
evidence.

The committed-checkpoint verifier is deliberately narrower than publication.
It accepts no source path and performs no journal transition: it reads and
exactly validates an already committed record and final artefact. `prepared`
and `materialized` operations remain operator evidence and are never advanced
by automatic reconciliation.

The committed restore-destination verifier has the same source-free,
read-only boundary. It additionally binds the trusted capture proof and exact
restore-generation journal input to the final object's persistent identity,
modeled content digest, and access-policy evidence. It never reopens the source
artefact or turns a visible destination pathname into activation authority.

This boundary supports only an approved local filesystem. NFS, other remote or
unknown filesystem semantics, canonical fence checks, stopped-writer
authentication, and non-cooperating same-UID races at the final POSIX rename
syscall are outside its guarantee. See
`docs/architecture/stopped-directory-publication.md`.

## Stopped-Directory Backend

The v3 stopped-directory backend composes the same-process capability, a
durable mutation-authority and catalogue seam, and local publication into the
snapshot core's storage-backend contract. It owns only `captureCheckpoint`
and `restoreCheckpoint` in the base contract, and exposes the optional v1
`reconcileCheckpointCapture` extension. When its mutation authority exposes
the matching optional contract, it also advertises
`preparedCheckpointCaptureContractVersion: 1` and provides fresh-only
`resumePreparedCheckpointCapture()` for a capture that the V3 stop handoff
already created as `prepared`. When the validated lifecycle backend declares
matching support, it delegates the optional version 1
`prepareRestoreAttachment` provider extension with the same backend ID.
Provision, writable attachment preparation, detach, force-fence, and destroy
operations remain delegated lifecycle work.

Capture consumes the exact stopped-writer capability once. While the
coordinator callback is active, the mutation authority holds the canonical
fence and admission guard, reserves a predetermined result, runs publication
exactly once, and durably finalizes the catalogue before returning that same
completion. Capture publication must atomically start from an absent journal
operation; it never adopts an earlier prepared, materialized, or committed
artifact as proof of the current stop. The production mutation authority keeps
publication outside database transactions while a durable reservation and
per-operation PostgreSQL session advisory guard serialize admission,
publication, finalization, and each reconciliation attempt. The advisory lock
is not durable across process, connection, or database restart, and reacquiring
it does not prove an older publication callback has quiesced. The retained
operation and session reservation prevent a second publisher; recovery remains
source-free and read-only until the physical journal proves the exact artefact
committed. A pre-commit journal phase therefore fails closed even if recovery
overlaps an older callback. Runtime
collaborator failures are fixed path-free uncertainty; the adapter performs no
internal retry, speculative cleanup, or replacement-coordinator recovery.

Inside the one-shot stopped-writer callback, the backend generates a fresh
capture-attempt UUID. Before publication, the authority must atomically claim
that exact UUID and operation in a globally unique durable ledger, create the
authenticated attempt, and retain non-reusable tombstones beyond every
journal, artefact, snapshot, backup, and DR generation that could restore an
old value. Active claim indexes must bind the same canonical attempt record;
retirement atomically changes both to non-authorizing tombstones. Claim
activation, reconciliation, finalization, and retirement share a per-operation
authority transaction or mutex, and finalization revalidates ownership after
asynchronous verification. A separate authority method can later load that
exact actively claimed record and ask the backend to verify only its committed
artefact. It never consumes another stopped-writer capability, reads the old
mutable source, or advances an uncommitted journal phase.

The adapter advertises normal directory attachments, exclusive writers,
`fencing: "manual"`, and
`atomicPointInTimeCheckpoint: false`. It is therefore a trusted local
filesystem development and conformance backend, not an NFS, live-volume, or
automatic failover implementation. The durable authority interface and
conformance tests define the seam, and the PostgreSQL authority now implements
clean capture, committed reconciliation, and typed destination-generation
state. The process-local launcher foundation now composes one-use image
consumption, external launch, exact provisional writer registration, and
no-relaunch attempt reconciliation. The atomic handoff additionally binds a
committed restore generation to an already-prepared durable launch attempt.
Detached activation now adds source-free destination verification, exact
provider attachment proof, atomic canonical attachment plus prepared launch,
an executable clean-detached intent-to-launch handoff, and four-lane
no-relaunch recovery. The backend's version 3 restore callback
can now carry the complete authority-issued generation binding to either fresh
publication or committed-only verification without changing the legacy version
2 callback. The database-global shared/exclusive lifecycle guard, bounded
recovery scheduler, invocation-time detached-production gate, foreground
composition, production-neutral runtime assembly, and same-launcher writer-
start ingress are now complete. The durable stable-plan registry, separately
gated provisioning facet, and private read-only foreground resolver are also
complete. Migration-before-serving, the initial complete recovery sweep,
restore admission, and shutdown drain now have one deployment owner.
Production restore remains fail-closed until later slices supply provider/
image, PostgreSQL connection/bootstrap configuration, and operational lease-
budget bindings, assembled restart/ambiguity validation, and final adapter
wiring. Filesystem-image execution and differential backup remain later work.
See
`docs/architecture/stopped-directory-backend.md`.

## Interrupted-Turn Recovery

The recovery probe starts a real Codex app-server against a held localhost
Responses API mock. It exercises six independent scenarios:

- stable `turn/interrupt`, followed by a cold resume;
- `SIGTERM` during an active turn;
- `SIGKILL` during an active turn;
- `SIGKILL`, a stopped full-tree copy, deletion of the source tree, and restore
  at a different absolute path;
- `SIGKILL`, removal of the final LF on a stopped-tree-derived writable copy,
  offline `append_lf` repair, resume, one completed follow-up, and a fresh cold
  read;
- `SIGKILL`, injection of an invalid unterminated tail on a stopped-tree-derived
  writable copy, offline `truncate_partial_tail` repair, resume, one completed
  follow-up, and a fresh cold read.

The probe verifies the explicit thread ID through both `thread/resume` and
`thread/read`. Explicit interruption persists a model-visible abort marker.
Signal and hard-kill recovery instead normalizes the stale in-progress turn to
`interrupted` without inventing that marker. The stopped-tree copy preserves
snapshot-user-accessible regular files and directories with their POSIX rwx
permission bits, plus portable UTF-8 symlink targets without following links.
Symlink permission bits are outside the modeled digest. Directory names must be
NFC-normalized. Inaccessible entries, non-ASCII cased names, case-insensitive
name collisions,
dangling relative links, relative-link
case or normalization aliases, traversal through non-directories,
resolution chains that cross protected trees, non-relocatable links, special
permission bits,
hard links (including hard-linked symlinks), sockets, FIFOs, and devices fail
closed. Ownership, ACLs,
extended attributes,
timestamps, and other unmodeled metadata are not preserved or covered by the
digest. If validation or copy fails after destination creation, the partial
destination is retained for cleanup by the trusted owner; the helper never
recursively removes a failure path that another writer could have replaced.
The copy helper requires exclusive single-writer control of its current-user-
owned, mode `0700`, extended-ACL-free root. It holds and revalidates that root,
and requires a trusted owner, permission, identity, and ACL state across the
complete ancestor chain. Concurrent mutation by another process with the same
UID is not a supported security boundary.
It is not an online, atomic, or power-loss-durable snapshot implementation.
The two repair scenarios preserve an immutable backup and use the same staged
private executable before and after repair. This proves
`same-pinned-executable` compatibility only; production OCI same-image
resolution, physical restore, and launcher admission remain separate work.

Run the deterministic compatibility probe with the exact Codex binary from the
pinned runtime image:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery
```

If the system temporary filesystem is mounted `noexec`, set
`CODEX_RECOVERY_EXEC_ROOT` to an existing absolute directory on an executable
filesystem with a trusted owner, ancestor chain, and ACL state; the probe
creates and removes its own mode `0700` subdirectory there.
The pinned macOS runtime must provide `/bin/ls` and `/sbin/mount`; Linux must
provide ACL-capable `/usr/bin/getfacl` plus `/proc/self/mountinfo`. The probe
invokes these fixed inspection surfaces and fails closed when they are absent,
malformed, or when raw Darwin mount paths contain text that is ambiguous with
the `mount(8)` output separators. Both platform tables are captured as bytes
and rejected before parsing if strict UTF-8 decoding would be lossy.

To update the redacted evidence after an intentional runtime upgrade:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery -- --write-evidence
```

The evidence parent directory must already exist, be owned by the current user,
and have trusted permissions, ancestors, and ACL state. Evidence publication
holds and revalidates that directory. A failure before rename retains its
private temp artifact for trusted-owner cleanup; a failure after rename leaves
the destination in place and the CLI reports
`evidence_durability_uncertain` without serializing exception details.

The command provisions no credential input and configures the model provider to
use the loopback mock. It does not impose OS-level outbound network isolation;
run it inside a network-isolated container when that stronger evidence is
required. See `docs/experiments/interrupted-turn-recovery.md` for source
evidence and exact probe semantics, and
`docs/architecture/rollout-tail-repair.md` for the repair contract and storage
limitations.

## Managed Auth Refresh Authority

A central authority can proactively rotate its managed ChatGPT credential
without starting a model turn. The reference adapter runs stable v2
`account/read {refreshToken:true}` against an isolated staging `CODEX_HOME`,
verifies the rotated record, and atomically promotes it into the dedicated
authority home. Concurrent in-process callers share one refresh execution.

The live probe intentionally mutates the dedicated login and then performs a
separate worker turn with the refreshed access token:

```bash
chmod 700 .test-codex-home
CODEX_BIN=/absolute/path/from/the/pinned-image/codex \
  CODEX_ALLOW_AUTH_MUTATION=1 \
  CODEX_ALLOW_UNCONTAINED_AUTH_PROBE=1 npm run probe:auth-refresh:live
```

Do not point this command at the default user `~/.codex` home or the active
`$CODEX_HOME`. The probe rejects path aliases and matching directory identities
and expects `.test-codex-home` or another dedicated authority login.
Production workers must not be able to mount, rename, or write the authority
home or its parent path; only the broker owns that single-attached volume.
See `docs/experiments/auth-refresh-authority.md` for the source evidence,
failure model, and production limitations.

## External Auth Compatibility Probe

The offline probe uses synthetic JWTs, an isolated temporary `CODEX_HOME`, and a
localhost Responses API mock. It verifies that:

- `chatgptAuthTokens` is rejected without `experimentalApi` opt-in.
- The same login succeeds with `experimentalApi: true`.
- A mocked `401 Unauthorized` triggers
  `account/chatgptAuthTokens/refresh`.
- The retried request uses the replacement access token.
- External auth does not create a worker `auth.json`.

Install the exact locked dependencies, then run the full Node test suite:

```bash
npm ci
npm test
```

The PostgreSQL authority integration applies the authority schema and creates
concurrency fixtures. It is excluded from default test discovery and must be
run explicitly against a dedicated disposable PostgreSQL 13-or-newer database:

```bash
SESSION_AUTHORITY_DATABASE_URL=postgresql://user@127.0.0.1:5432/portable_codex_runtime_test \
  npm run test:postgres
```

The GitHub Actions integration job supplies a fresh PostgreSQL 18.4 service and
this URL. The explicit command fails when the URL is absent, so that gate cannot
pass by silently skipping the test.

Two external-auth app-server integration tests run when `CODEX_BIN` (or
`codex` on `PATH`) is executable. The third app-server integration test is the
full interrupted-turn recovery matrix; it requires `CODEX_BIN` to be an
explicit absolute path so the probe can bind evidence to that exact binary.
Unavailable integration tests are reported as skipped on Node-only CI runners;
the remaining tests still run normally.

The reference host app-server runtime currently supports macOS and Linux process
groups. A process can escape that group by creating a new session, so this is not
production containment for credential-bearing refresh. Windows is rejected
before reading managed credentials, creating a worker home, or spawning Codex;
`ChildProcess.kill()` alone is not treated as process-tree isolation.

Run the offline protocol probe and print a JSON report:

```bash
npm run probe:external-auth
```

Set `CODEX_BIN` to test a specific Codex executable:

```bash
CODEX_BIN=/path/to/codex npm test
```

A bare executable name such as `codex` is resolved through `PATH`; relative and
empty `PATH` entries are anchored to the launcher working directory before the
app-server switches into its isolated `CODEX_HOME`. A relative value containing
a path separator, such as `./bin/codex`, is likewise resolved against the
launcher working directory.

## Live External Auth Probe

The live probe reads a dedicated managed ChatGPT login from
`.test-codex-home/auth.json`, injects only its access token into a temporary
worker, and sends one fixed, non-repository prompt to the real Codex backend.
It does not pass the refresh token to the worker or modify the source auth file.

The dedicated auth home is ignored by Git. On success, the probe writes a
redacted record to `evidence/live-external-auth.json`. The tracked evidence
contains runtime metadata and the final status only; it omits credentials,
emails, complete account/workspace identifiers, and token-derived fingerprints.

Run the live probe explicitly:

```bash
npm run probe:external-auth:live
```

Optional overrides:

```bash
CODEX_TEST_HOME=/path/to/dedicated-codex-home \
CODEX_LIVE_PROBE_MODEL=gpt-5.4 \
CODEX_LIVE_EVIDENCE=evidence/live-external-auth.json \
npm run probe:external-auth:live
```

## Repository Automation

The default branch contains `.github/workflows/codex-review-gate.yml`. Pull
requests use the `codex/review-gate` check supplied by the repository template.

The retained `scripts/setup-ci.mjs` generator can add project-specific CI and
tooling later. Inspect planned writes before enabling a module:

```bash
node scripts/setup-ci.mjs --list
node scripts/setup-ci.mjs --tool github-actions --tool markdown --dry-run
```

Its generator tests are included in the default `npm test` command.

## Project Records

- Current repository state: `docs/PROJECT_STATE.md`
- Cross-workstream backlog: `docs/PROJECT_TODO.md`
- Workstream journals: `docs/project_journal/`

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.
