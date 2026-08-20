# Project TODO

- [done] Prove a central auth authority can refresh credentials without a
  normal model turn and preserve the successful method with redacted evidence.
- [done] Characterize interrupted and killed Codex turn recovery from the
  pinned runtime and filesystem snapshots.
- [done] Define session filesystem, manifest, lease, fencing, and pluggable
  storage contracts for rootless workers.
- [done] Implement the central auth broker so session workers receive access
  tokens without mounting shared refresh-token state.
- [done] Implement backend-neutral stopped-writer clean checkpoint and restore
  orchestration independently of auth state.
- [done] Extract reusable stopped-tree validation, copy, digest, and guarded
  cleanup primitives without claiming a durable snapshot backend.
- [done] Implement the durable host-local filesystem operation journal with
  canonical phase and exact committed-result replay.
- [done] Implement local stopped-directory storage barriers, deterministic
  staging, atomic checkpoint-bundle and restore-tree publication, exact
  readback, and pre-commit consumer isolation against the journal phases.
- [done] Implement the same-process stopped-writer capability coordinator with
  one trusted stop, one object-identity capability, and one snapshot callback.
- [done] Compose the journal, publication layer, one-use stop capability, and
  durable mutation-authority/catalogue seam into the stopped-directory backend
  adapter and conformance suite.
- [done] Add authenticated durable capture-attempt provenance and source-free,
  committed-only uncertain-outcome reconciliation.
- [done] Verify same-pinned-executable resume and implement offline
  rollout-tail repair for the pinned plain-JSONL framing contract.
- [done] Add a bounded OCI/Docker runnable-image and Codex-executable
  reservation plus the PostgreSQL serializable transaction, schema, and
  real-concurrency foundation for central runtime authority.
- [done] Implement the durable operation and reservation kernel for exact
  idempotent replay, conflict exclusion, and explicit uncertain-outcome
  reconciliation.
- [done] Implement database-clock writer lease acquisition and renewal plus
  exact attachment finalization with monotonic uint64 fencing epochs.
- [done] Implement release, force-fence, `FENCING`, and `BLOCKED`
  reconciliation without treating database epoch allocation as a physical
  fence.
- [done] Compose provider-backed canonical writer detach behind one
  per-operation PostgreSQL advisory guard. Invoke storage only from a definite
  typed dispatch grant, validate exact detach or force-fence proof, preserve
  finalizer acknowledgement-loss replay, and fail retained ambiguous dispatch
  state closed to `BLOCKED` without replaying the provider.
- [done] Implement the production clean-capture mutation authority and
  checkpoint catalogue on the existing PostgreSQL schema, including
  source-free committed reconciliation and permanent attempt claims.
- [done] Implement a bounded operational recovery enumerator and service
  loop for retained `starting` or `uncertain` checkpoint capture operations.
  Recover the exact checkpoint and mutation request only from durable
  operation state, use stable artefact-root resolver configuration, and leave
  unverifiable or guard-busy attempts durably blocked for later retry.
- [done] Add the ordered checksum-bound PostgreSQL migration chain and the
  permanent relational schema foundation for canonical restore destination
  generations.
- [done] Implement typed canonical restore destination generation claim,
  dispatch, finalisation, exact replay, and recovery authority while keeping
  production restore fail-closed in that authority-only slice.
- [done] Implement the durable launch-attempt lifecycle around exact
  generation, lease, attachment, fencing, measured-image, process, writer, and
  supervisor bindings without invoking a launcher inside that slice.
- [done] Add the logical-writer-launcher foundation: consume the original
  one-use measured-image capability only after durable `starting`, invoke an
  external launcher once, register a provisional writer before started
  finalisation, reconcile active attempts without relaunch, and add typed
  launch-stop authority plus bounded prepared/active/current-launch discovery.
- [done] Eliminate the generation-to-launch crash gap with a version 2 restore
  request that durably binds one launch intent, one serializable transaction
  that commits the generation and reserves that exact launch attempt, and a
  launcher path that consumes only the already-reserved attempt.
- [done] Compose one same-process durable writer stop with one clean capture:
  bind the complete prepared capture tuple to the exact
  `writer-launch-stop-v1` identity, validate its committed transition before
  issuing the one-use capability, reconcile same-process pre-dispatch stop
  replay, block same-session successor launch through capture, and retire local
  writer identity only after confirmed capture success.
- [done] Compose detached restore activation and recovery: source-free verify
  only an exact committed destination, bind version 1 provider attachment
  evidence without treating pathname equality as authority, atomically install
  the canonical attachment plus prepared launch, and sweep five bounded
  generation, activation, launch, current-inventory, and
  `supervisor-state-gc` lanes without relaunching.
- [done] Add a fleet-gated capture-bound activation request that proves
  the exact current-writer stop, clean checkpoint capture, and subsequent
  release or force-fence before a distinct target restore generation can
  activate its detached destination. Preserve existing activation v1 replay.
- [done] Persist one independent generation, activation, launch-attempt, and
  current-launch recovery cursor per configured scope, and add a bounded
  single-flight runner that durably advances each settled lane before admitting
  the next. That slice kept the runner unscheduled and production restore
  fail-closed pending later assembly.
- [done] Add a version 3 stopped-directory restore callback that passes the
  complete authority-issued generation binding to fresh publication or
  committed-only verification while preserving the legacy version 2 callback.
- [done] Admit capture-bound activation through the separately fleet-gated
  detach-to-generation-to-capture-to-stop predecessor chain while preserving
  the old durable topology, and let the real launcher prepare and consume an
  activation-materialized prepared launch without a second reservation or
  physical launch on replay.
- [done] Close the stop-to-capture restart gap with writer-stop request V3:
  preclaim the capture operation ID before physical stop, atomically commit the
  stop and materialize its exact prepared capture, permit fresh publication
  only after one definite cold dispatch grant, and keep starting or uncertain
  recovery committed-only. Preserve the legacy same-process capability path
  and keep fresh V3 reservation default-denied by its own fleet decision.
- [done] Add a cross-process shared/exclusive restore lifecycle guard and a
  bounded production recovery scheduler so foreground prepared launch cannot
  race recovery cancellation.
- [done] Complete production restore adapter enablement. Phase A
  provides the caller-persisted stable plan, invocation-time default-deny fleet
  gate, and shared-lifecycle foreground composition seam across committed
  publication, durable stop/capture, canonical detach, activation, and
  prepared launch. The production-neutral assembly now constructs that private
  facade, its private capture backend, an immutable public backend, three
  distinct lifecycle/operation guard pools, authority/store pool, idle bounded
  no-relaunch scheduler, and a narrow same-launcher writer-start ingress. The
  PostgreSQL durable plan registry, separately gated provisioning facet, and
  private read-only foreground resolver are now complete. The deployment
  controller now owns migration-before-serving, the initial complete recovery
  sweep, checkpoint-backend and image-reservation admission, scheduler
  shutdown, and admitted-call drain while leaving the four low-level pools
  borrowed. The
  production deployment factory now owns explicit PostgreSQL connection/
  bootstrap configuration, constructs those four private pools, performs a
  point-in-time same-primary topology check before controller startup, and
  closes every pool after controller drain. The deployment-owned image-plan
  binding now resolves an authentic plan's `imagePlanId` to exact OCI bytes,
  trusted inspection, and one opaque reservation shared by foreground
  preparation and launcher revalidation. The settlement foundation now applies
  separate result deadlines and post-deadline grace periods to image resolution
  and inspection, supplies provider contract version 2 with a fresh authentic
  abort signal plus opaque invocation identity, and routes a no-settlement
  breach only to deployment-owned fatal shutdown without claiming physical
  quiet or retry authority. Activation now also keeps claim, reconciliation,
  optional first attach, and finalization inside one coordinator-owned
  per-operation guard; retained, ambiguous, or copied caller state cannot
  reconstruct a second attach dispatch. A deployment-private physical binding
  graph now gives the three supervisor methods, a separate supervisor-state
  collector, nine storage-lifecycle methods, four publication methods, and
  restore-destination resolver independent
  deadlines and grace periods while preserving existing durable authority.
  Deployment now derives the two database-clock critical-window bounds from
  those method policies, an explicit aggregate database allowance, and a
  positive safety margin; stable-plan provision and every resolution enforce
  the exact admitted lease. The completed version 2 safety matrix classifies
  all twenty settlement leaves as nine protocol-surface mutators, six
  observations, and five contract-only leaves; it maps the nine mutators to
  nine real-PostgreSQL acknowledgement-loss paths and binds a
  same-database/stable-plan retry
  through fresh physical bindings, image binding, runtime, and controller plus
  separate registry rehydration, an explicit test-only publication-seam router,
  and representative settlement timer/drain evidence. The final immutable
  public checkpoint backend is now assembled over a second private stopped-
  directory backend and exposed only through controller/deployment admission.
  Callers cannot inject the internal generation-publication callback, invoke
  raw lifecycle mutations, or reach operator/provider extensions.
- [done] Implement production-injectable Linux ext4 physical components:
  sparse raw-image lifecycle below host-owned `rprivate` carriers,
  close-before-unmount clean detach settlement, externally anchored provider
  state with automatic checkpoint/delta-log generation rotation and capacity
  inspection, distinct archive mount-root and artifact-child publication-
  control identities, rootless digest-pinned Podman launch/stop, and two-host
  clean detach, transfer, verification-only first remount, identity
  verification, and a peer-namespace non-propagation gate.
- [done] First, bind the provider state's committed ext4 attachment identity
  into a trusted Podman filesystem authority and exercise that bridge with the
  initialized backend in one non-root process. This closes the current
  production identity gap without depending on either retention track below.
- [done] Second, add authority-owned bounded retention or garbage collection
  for terminal local supervisor state. Migration 009 permanently records exact
  owner-finalizer authorization, an immutable pre-dispatch route to the
  private root's persistent `state-owner:<64 lowercase hex>` marker, and
  collection completion. First owner preparation atomically publishes only a
  fully written and synced marker-bearing root from a unique same-parent
  staging directory; pre-rename crashes leave inert debris, while a retry
  after rename or acknowledgement loss adopts the exact complete marker.
  Existing malformed or unmarked final roots remain fail-closed. The third and
  fifth recovery lanes are filtered by
  that local owner, while the assembled production cold-start runner hashes
  the caller's base recovery scope with the marker for cursor isolation and
  holds the database-global exclusive lifecycle guard, with a dedicated
  physical settlement. Migration 009 refuses active legacy
  `starting`/`uncertain` launches and any non-null session current-launch
  pointer, including malformed or orphaned pointer data, before installation.
  That gate requires a committed current launch to be stopped or physically
  fenced and its current-launch pointer cleared before rollout. It then fences
  ownerless dispatch from already-running old binaries with a deferred
  commit-time constraint. Owner updates are forbidden, while deletion requires
  same-transaction teardown of the permanent operation-ID claim; no runtime
  API exposes partial owner deletion or rebinding. Historical unbound
  terminal work that is no longer current is not retroactively authorized and
  remains an explicit cleanup case. The collector deletes exact stopped
  revisions in two directory-
  synced phases and accepts acknowledgement-loss replay from `collected` to
  `absent`. Its fifth-lane cursor persists
  `(sessionId, authorizedAt, terminalOperationId)` rather than collapsing each
  session to its oldest item, so a pending item cannot starve later work from
  that session before the next wrap. On Linux, destructive artifact operations
  resolve through a revalidated held-root FD clone; non-Linux retains explicit
  pathname brackets without claiming active same-UID ABA resistance.
  Production deployment accepts only the exact process-local
  supervisor/collector pair returned by
  `createPodmanWriterSupervisorBundle()`; matching IDs and owner strings are
  necessary but insufficient, and direct `createPodmanWriterSupervisor()` is
  caller-asserted/raw only. Owner preparation and state/supervisor bundle
  construction fail closed before physical dispatch. Runtime fixes that owner
  into foreground launch-attempt reads as exact
  `{ operationId, stateOwnerId }`, outside public restore admission.
  Only the exact durable revision 4 cold-reconciliation branch may retire its
  stopped container with idempotent removal plus exact name/ID absence proofs,
  return the terminal record, and use the owner-bound GC finalizer. Ambiguous
  removal, proof, adaptation, or a pre-commit finalizer failure preserves
  revision 4 and commits no database finalization. A post-COMMIT
  acknowledgement loss may instead follow an atomic commit of the operation
  and owner-bound GC authorization; exact authorization readback determines
  whether that commit exists. Revision 4 remains until the authorized collector
  removes it in either case. Observer-only `complete-stopped`/`not-started`
  remains null-record and no-GC. This marker is routing identity, not
  cryptographic host
  attestation or protection against an administrator cloning both root and
  marker.
- [done] Third-a, add the authority-safe PostgreSQL operation-index foundation.
  Migration 010 stores bounded canonical prepared and committed record bytes,
  the prepared checksum, explicit committed-checksum provenance, and domain-
  separated record digests behind the external provider head. Native commits
  use `indexed-frame-v1`; migration 010 neither represents nor permits an
  unavailable rotated-legacy suffix. A nullable head marker records the exact
  state revision whose operation index is complete. Migrated version 2 heads
  remain unadopted;
  operation reads, paging, and exact-head appends fail closed until adoption.
  Genesis-created indexed heads advance the marker with each logical append
  and retain it across rotation. A serializable head CAS and record
  insert/update form one durable cut. The latest validated committed record is
  the current storage projection, and a full canonical before-state mismatch
  rolls back the head, marker, and history. Destroyed storage remains a
  committed tombstone. Prepared history may acquire its committed
  suffix exactly once, and operation deletion requires same-transaction
  teardown of the complete anchor; whole-table truncation is forbidden. The
  version 2 migration already requires every non-null value in the three
  external-head checksum columns to be an exact 64-byte lowercase-hex value.
  Migration 010 normalizes those valid values to `varchar(64)` before version 3
  and gives all four new operation checksum/digest columns the same exact
  format. Existing valid version 2 heads remain unchanged and production
  serving is unchanged.
- [done] Third-b, switch provider state to version 3. Migration 011 introduces
  the `unavailable-adopted-v2` suffix only inside one database-identified
  adoption transaction. Under the provider lock, the complete version 2
  reducer proves revision coverage, per-storage lineage, pending uniqueness,
  final storage, and attachment origins; the filesystem writes and verifies a
  covering version 3 checkpoint before PostgreSQL atomically imports or verifies
  every operation and advances the head plus completeness marker. A deferred
  database trigger rejects incomplete, duplicate, extra, or cross-manifest
  cuts, while an internal unique event-revision registry makes later indexed
  completeness inductive across one-revision appends and pure rotations. A
  database-managed progress transaction ID fences a second head mutation after
  an early constraint check, and stored heads exclude canonical revision zero.
  Exact readback settles commit acknowledgement loss. Version 3
  checkpoints retain current storage, destroyed tombstones, attachment-origin
  IDs, and the live prepared recovery working set, but no committed history.
  Arbitrary-age exact replay comes from the permanent PostgreSQL index.
- [pending] Add a streaming or paged adoption contract for otherwise-valid
  version 2 state containing more than 65,535 operations, 65,535 storages, or
  64 MiB of aggregate canonical operation/prepared-projection/storage
  material. The current full-array adoption v1 contract rejects that state
  with `state_capacity_exhausted` before any candidate generation mutation;
  its operational capacity must not be described as the version 2 format
  limit.
- [pending] Extend beyond that clean/manual-fencing boundary only in separately
  scoped work: power-loss/crash-prefix evidence, automatic stale-writer
  fencing, differential export/compression, content-addressed distribution,
  encryption, retention and periodic snapshots, registry publisher/signature
  trust, and remote image transport.
- [deferred] Add a read-only Git Summary for user context; it is not part of
  snapshot correctness or recovery.
