# Project State

## Current State

- The repository contains the Codex review-gate template and external-auth
  compatibility probes for the portable runtime.
- A managed authority can proactively refresh ChatGPT credentials through
  `account/read` without a model turn, then atomically promote verified state.
- Explicit turn interruption and process-kill recovery semantics are captured
  by a deterministic real-app-server probe with redacted evidence.
- An offline rollout-tail repair primitive now validates the complete stopped
  session set, preserves valid JSONL bytes, appends one missing final LF or
  truncates one invalid unterminated tail, and fails closed on non-tail
  corruption or unsafe filesystem state. The live probe binds one Codex
  executable by version and SHA-256 before and after repair; it does not claim
  OCI same-image recovery or production launcher authority.
- Versioned session manifest, storage attachment, lease/fencing, structural
  rootless worker template, and checkpoint class contracts define the portable
  data-plane boundary without exposing raw devices to workers or claiming
  metadata-only physical authority.
- An encrypted canonical auth store and generation-aware broker now provide
  claim-validated access-token delivery, single-flight refresh, exact commit
  reconciliation, canonical-path coordination, pre-dispatch recovery
  reservations, stale-worker suppression, and durable reauth/recovery gates
  without placing refresh-token state on session volumes.
- A backend-neutral snapshot and restore core now orchestrates stopped-writer
  clean checkpoints, requires a newer restore epoch, and fails closed on
  uncertain backend outcomes without claiming a physical snapshot backend.
- Stopped-tree validation, copy, digest, and guarded cleanup are reusable
  independently of the recovery probe, without claiming atomic publication or
  power-loss durability.
- A host-local canonical operation journal now durably records exact prepared,
  materialized, and committed states and replays committed results after
  restart without claiming that physical backend work occurred.
- A local stopped-directory publication layer now binds journal phases to a
  post-order source barrier, deterministic durable staging, checkpoint bundles
  or payload-only restores, atomic absent-destination rename, exact final
  readback, and pre-commit consumer isolation.
- A same-process stopped-writer coordinator now converts one trusted, fully
  joined writer stop into one object-identity capability for one snapshot
  callback without making protocol events or serialized fields into authority.
- A v2 stopped-directory backend now composes that one-use capability with a
  durable mutation-authority/catalogue seam and local publication. It delegates
  lifecycle operations, guards exact predetermined capture/restore results,
  atomically starts normal capture from an absent journal operation, and fails
  closed on pre-existing publication state, callback uncertainty, or
  finalization uncertainty while declaring local-filesystem and manual-fencing
  limits.
- Normal capture now durably binds an authenticated capture-attempt ID before
  publication. The optional v1 reconciliation extension can load that
  canonical attempt and source-free verify only its exact committed journal
  record and artefact; it never reuses a stopped-writer capability or advances
  `prepared` or `materialized` evidence.
- The authority foundation now supplies a same-client PostgreSQL
  `SERIALIZABLE` executor, database transaction time, provenance-aware bounded
  retry, a checksum-bound control-plane schema, and real PostgreSQL concurrency
  coverage. A bounded OCI/Docker runnable-image resolver binds exact
  descriptor and config bytes, layer/rootfs structure, and measured Codex
  executable identity to a one-use process-local reservation.
- A canonical PostgreSQL session registry now persists one immutable manifest,
  storage reference, and backend capability set in an initial `DETACHED`
  document. Exact registration replay is idempotent, conflicting reuse fails
  closed, and strict readback returns a frozen validated snapshot. Registration
  does not allocate a writer lease, attach storage, or authorize a launch.
- A durable PostgreSQL operation/reservation kernel now claims one exact
  canonical request under a conservative session-wide conflict class, binds
  the active rows to the session pointer and revision, grants dispatch only
  through one definite `prepared -> starting` CAS, and retains `starting` or
  `uncertain` blockers across replay and restart. Only a still-`prepared`
  operation can be cancelled generically. A versioned terminal anchor binds
  each progressed inactive session to its latest committed operation and
  released reservation while preserving legacy request hashes.
- A typed PostgreSQL writer-acquisition layer now reuses generic reservation,
  then atomically claims `prepared -> starting` after locking the exact
  session, operation, and reservation state. It reads `clock_timestamp()` at
  that decision point, allocates a bounded lease, deterministic lease and
  attachment IDs, and the next uint64 epoch, and persists `ATTACHING`.
  Dispatch is granted only when enough PostgreSQL bigint revision capacity
  remains to record an uncertain outcome and its exact finalization. Providers
  remain outside transactions. Exact attachment evidence, including a
  provider-result binding for the canonical host-local `rootPath`, can finalize
  `starting` or `uncertain` to `ATTACHED`, including after lease expiry, while
  retiring the operation, releasing the reservation, clearing the active
  pointer, and writing the terminal anchor. Database-clock renewal is
  exact-operation idempotent and changes only `expiresAt`; expiry equality
  fails closed, and neither expiry nor epoch allocation proves a physical
  fence.
- Typed writer release and force-fence reconciliation now reuse the same
  schema and generic reservation kernel. Exact-owner release preserves the
  lease tuple and epoch and may finalize a matching detach after expiry.
  Force-fence dispatch starts only from `ATTACHED` or `BLOCKED`, advances the
  uint64 epoch at its definite dispatch commit, and enters `FENCING`; only the
  dedicated exact provider proof reaches `DETACHED`. Ambiguous or unavailable
  outcomes finalize to `BLOCKED` while retaining the tuple, target, and current
  epoch, and explicit recovery dispatch is required for
  `BLOCKED -> FENCING`. Manual backends cannot complete automatic fencing, and
  database expiry or epoch state is never physical fence evidence.
- A provider-neutral PostgreSQL writer-detach composition now binds one exact
  release or force-fence request to the per-operation advisory guard, typed
  authority dispatch, backend-generated proof, and durable terminal state.
  Provider invocation occurs only after a definite dispatch grant and outside
  every database transaction. Replayed `starting` or `uncertain` state is never
  a second dispatch grant: storage contract v1 has no provider reconciliation
  operation, so the facade records a durable `BLOCKED` result instead. A valid
  proof survives database finalization acknowledgement loss through exact
  readback/finalizer replay, and manual fencing records `fence-unavailable`
  without invoking the provider. This detach-only facade remains unscheduled;
  production restore was still fail-closed at that historical slice boundary.
- Production clean-checkpoint capture authority now reuses the version 1
  PostgreSQL schema without DDL. One exact durable operation and reservation
  binds the canonical stopped-writer admission, globally unique capture
  attempt, predetermined result, and checkpoint catalogue entry. Publication
  runs outside database transactions while a per-operation PostgreSQL session
  advisory guard serializes each live invocation. The durable reservation and
  attempt claim prevent a second publisher, but guard reacquisition after a
  process, connection, or database restart does not prove the older callback
  quiesced. Same-operation recovery therefore stays source-free and read-only
  until it verifies the exact committed journal state; it cannot advance
  `prepared` or `materialized` publication. Claims are retained permanently,
  and tombstones always reject reuse.
- Writer-stop request version 3 closes the durable gap before that capture
  operation exists. Migration 006 preclaims the exact capture operation ID
  before stop dispatch; one serializable finalizer commits the complete stop
  and materializes the matching prepared capture and active session pointer.
  Fresh V3 reservation is independently default-denied. Cold publication
  requires a definite prepared-to-starting grant and uses only fresh journal
  creation; starting or uncertain recovery stays committed-only.
- Bounded checkpoint recovery now enumerates retained handoff `prepared`,
  `starting`, or `uncertain` capture operations through
  `PostgresSessionAuthority.listCheckpointCaptureRecoveryCandidates()`. It uses
  immutable `session_id` keyset order, the existing active-operation index, a
  hard `limit + 1` page query, and same-snapshot relational validation without
  schema DDL. The single-backend recovery service processes one page
  sequentially from frozen startup configuration. It routes an exact handoff
  `prepared` admission to the optional fresh-only resume callback, while
  `starting` and `uncertain` admissions use committed-only reconciliation. Its
  `reconciled` and `pending` receipts advance the cursor only after settlement;
  sweep completion wraps to null for later replay. Abort signals stop new
  admission but drain the in-flight guard/reconciliation. A service instance
  admits only one batch at a time, while overlapping valid invocations fail
  closed before enumeration. Guard-busy or unverifiable attempts remain
  durable blockers and `starting` may safely become `uncertain`.
- The PostgreSQL authority now applies an ordered, checksum-bound migration
  chain whose installed ledger must be an exact contiguous prefix. Migration
  version 2 adds a permanent `restore_destination_generations` relation with
  independent generation and operation identities, same-session operation and
  checkpoint foreign keys, and exact authorized/committed row-shape
  constraints. Migration version 3 adds the permanent global operation-ID
  registry, backfills direct operations, and requires a version 2 restore to
  claim its launch-attempt ID before publication dispatch.
- Typed restore-generation authority now reserves the exact
  `{checkpoint, request}` admission, claims one generation and
  destination-isolation proof under the database-clock lease and restore
  fence, and atomically finalises the committed generation with its operation,
  reservation, and session terminal anchor. Claim replay never grants a second
  dispatch; exact finalisation replay and bounded `starting`/`uncertain`
  recovery remain available through the ordered migration chain.
- Typed durable writer-launch attempts reuse the permanent operation and
  reservation lifecycle rows. One globally registered operation ID is also the
  launch-attempt ID and binds a committed destination generation, exact
  attachment, stable lease and fence tuple, bounded measured-image projection,
  and trusted supervisor identity. Definite claim revalidates the generation's
  committed session-history relation, acquires all relation locks, then checks
  database-clock lease validity before granting dispatch once; `starting` and
  `uncertain` remain durable blockers. Exact supervisor evidence finalises an
  attempt as started, not started, or completely stopped.
- Canonical session document version 3 adds a relationally checked current
  launch pointer for a started writer. The pointer remains authoritative after
  lease renewal, checkpoint capture, or another terminal operation replaces
  `lastOperation`; readback follows its operation ID and revalidates the
  permanent request, result, generation, attachment, stable lease tuple, image
  measurement, process incarnation, writer incarnation, and supervisor proof.
  Version 1 and version 2 documents retain exact-read and replay compatibility,
  while the next real state write upgrades them to version 3.
- A hardened PostgreSQL logical-writer-launcher facade now revalidates the
  original image reservation, durably reaches `starting`, consumes that exact
  one-use capability, invokes one external launch callback, and registers a
  provisional same-process writer before finalising and exposing a started
  result. Prepared recovery cancels without launch. For `starting` or
  `uncertain`, an exact local provisional record retries started finalisation;
  otherwise recovery consults only the stopped-only supervisor path and never
  relaunches. Committed outcomes must match their canonical revisions. A newly
  finalised receipt requires a complete `lastOperation` anchor; historical
  readback may instead coexist with a later active operation or retain a later
  committed anchor while the digest-bound current launch pointer still binds
  the original attempt. The facade routes a capture-bound physical stop
  through the durable stop transition and retains its strong attempt,
  attachment, and writer indexes until the exact clean capture succeeds. An
  uncertain stop, finalisation, capture, or retirement retains the record
  fail-closed instead of treating a possibly running or incompletely captured
  writer as reclaimable. Within one canonical same-process stopped-writer
  coordinator, any retained writer blocks physical successor launch for that
  session across backend and storage slots until explicit retirement. Exact
  stop retries preserve an exact frozen operation input until the locked
  authority proves both the operation and its ID claim absent. A strictly newer
  same-incarnation session may then replace that input only after the complete
  stop relation and lease expiry remain monotonic from the retained
  precondition. Stop request contract version 2 persists only a
  domain-separated digest of a high-entropy dispatch claimant token; the raw
  token stays in the local writer record and is required by typed claim and
  reconciliation. Exact legacy version 1 requests remain readable and
  finalizable with their original edge-only claim semantics. Durable v2
  `prepared` or token-matched `starting` state reconciles without repeating
  physical stop. A local attempted-claim witness plus the durable token match
  recovers COMMIT acknowledgement loss, while a pre-commit failure, foreign
  token, explicit mismatch, or never-attempted `starting` state remains
  closed. A lease renewal may extend expiration while
  the stable registered writer-fence tuple remains exact; expiry rollback or
  identity drift rejects before stop reserve.
  After one confirmed physical stop, exact reconciliation selects revision 2
  only for an authority-proven `uncertain` stop and otherwise retains revision 1
  for terminal replay; neither path repeats the physical callback.
  Intent preparation now also accepts an exact clean version 3 `DETACHED`
  session after release or force-fence without reserving an operation or
  consuming the image. An activation-materialized prepared attempt then runs
  through the existing launcher exactly once; replay neither reserves nor
  physically launches again.
- Typed `writer-launch-stop-v1` authority preserves the original started
  attempt and clears the current-launch relation only for exact
  `complete-stopped` evidence from the bound supervisor. Historical stop or
  claim replays expose a current launch only when it still belongs to that
  stopped attempt, so a successor remains visible in the session snapshot but
  is never attached to the old receipt. Bounded keyset discovery returns
  prepared or active launch attempts and relationally validated current
  launches without reconstructing process-local authority.
- A same-process durable stop-to-clean-capture composition now prepares the
  exact capture admission before stopping, derives the stop identity from the
  complete attachment/checkpoint/request tuple and launch attempt, and permits
  one opaque capability only after the supervisor's physical stop and complete
  committed transition validate. Capture success retires the retained local
  identity; ambiguity never reissues the capability, repeats physical stop, or
  reconstructs a handle.
- A separate V3 path returns no opaque capture capability. It atomically
  materializes the prepared capture with the stop, retains local launch
  exclusion through publication, and permits retirement only after the exact
  predetermined committed capture result validates. This preserves the legacy
  V1/V2 capability path while making the new handoff discoverable after a
  process restart.
- Restore-generation request version 2 durably records the exact measured
  image, supervisor, and launch-attempt identity before publication begins.
  One serializable authority transition commits the generation, retires the
  restore operation, creates the exact prepared launch operation and
  reservation, and advances the canonical session twice. The launcher can
  prepare the process-local image capability before publication and later
  claim only that pre-reserved attempt, so a crash cannot leave a newly
  committed generation without discoverable launch work.
- Detached restore activation and recovery composition now verifies only an
  exact committed destination without re-reading a capture source or advancing
  publication state. The optional version 1 provider extension binds that
  published object and materialization digest to new attachment evidence.
  Typed `restore-attachment-activation-v1` authority then serializably installs
  the exact attachment and materializes its predeclared prepared launch as one
  atomic canonical transition. Historical activation request version 1 keeps
  its direct stop-to-detach replay relation. Request version 2 instead binds
  the actual production predecessor chain—committed current-writer stop,
  committed clean capture, then release or force-fence of the same old
  attachment—without requiring the stopped launch generation to equal the
  target restore generation. Its historical backward topology remains
  detach-to-capture-to-stop compatible. Fresh work may now use the separately
  gated detach-to-generation-to-capture-to-stop topology, with a committed
  version 1 target generation between capture and detach.
- Fresh restore-generation-v2 and capture-bound activation-v2 reservation are
  independently default-denied at the PostgreSQL authority boundary. Explicit
  startup compatibility decisions permit only their matching request version.
  The generation-predecessor activation-v2 topology has its own independent
  default-closed fleet gate, while old topology and exact existing replay remain
  available after that gate closes.
- A bounded restore recovery coordinator and service now cover retained
  destination generations, attachment activations, prepared or active launch
  attempts, current-launch inventory, and authorized terminal supervisor-state
  collection through five independent keyset cursors. Recovery is source-free
  and no-relaunch: it never republishes, reserves or consumes an image, invokes
  a launcher, or reconstructs an opaque writer capability. Current-launch
  handling is inventory only.
- PostgreSQL now persists those five cursors independently for each effective
  recovery scope. The first four lanes retain a scalar session cursor. The
  fifth lane persists the complete
  `(sessionId, authorizedAt, terminalOperationId)` ordering key, so one
  permanently pending authorization cannot hide later work from the same
  session; wrap retries it in a later cycle. Runtime derives that opaque scope
  with domain-separated
  SHA-256 from the caller's base `recoveryScopeId` and the local persistent
  `stateOwnerId`; the base remains a fairness/replay label and is never treated
  as root identity. This prevents two roots reusing one base label from
  advancing each other's third/fifth-lane cursors while keeping same-root
  restart stable. A bounded single-flight runner advances each settled lane by
  revision/cycle compare-and-swap, survives restart and commit-acknowledgement
  loss, and preserves earlier lane progress when a later lane fails.
- Migration 009 permanently records terminal local supervisor-state GC
  authorization and completion against both the terminal operation and launch
  attempt, plus one immutable pre-dispatch owner binding in
  `writer_supervisor_state_owners`. Each private state root supplies a
  persistent high-entropy marker matching `state-owner:<64 lowercase hex>`.
  First preparation creates the complete canonical marker in a unique sibling
  staging directory, syncs marker, candidate root, and parent, then renames the
  complete directory to the final root and repeats those durability barriers.
  Pre-rename crashes leave only inert staging debris; post-rename or lost-ack
  retries adopt only a complete exact marker. Existing malformed or unmarked
  final roots remain fail-closed and are not repaired in place. Because Node
  exposes plain POSIX `rename()` rather than `RENAME_NOREPLACE`, this does not
  exclude an active same-UID process inserting an empty final root in the last
  absence-check window. Node also has no inode-conditioned `unlink`/`rmdir`;
  Linux loser cleanup uses held-FD `nlink == 0` as a post-operation proof,
  while non-Linux hosts retain held/name brackets and durable name absence
  without claiming active same-UID ABA resistance.
  Owner updates are rejected immediately. Owner deletion is accepted only
  with same-transaction teardown of the permanent operation-ID claim, so
  delete-and-reinsert cannot transfer recovery authority to another root.
  Only an owner-bound finalizer that commits exact `complete-stopped` evidence
  with its stopped revision 4 `terminalRecord` may create that authorization.
  Immediate launch/stop and exact revision 4 cold retirement use that path;
  observer-only reconciliation returns a null terminal record and remains
  no-GC. In the assembled production runner, the
  fifth lane pages pending rows last while that runner holds the database-global
  exclusive lifecycle lease, invokes the separately settled physical collector
  with exact `{ stateOwnerId, terminalRecord }`, verifies the version-2 receipt
  carries the same owner before completion, and then records `collected` or
  `absent`. The third and fifth authority lists are owner-filtered by
  runtime-private wrappers, so callers cannot select a foreign root. An
  acknowledgement-loss retry may observe `collected` followed by `absent`
  without losing the durable completion. The destructive collector's grace
  expiry aborts and reports fatal failure but does not release its invocation,
  aggregate physical shutdown, or the normally held lifecycle lease until the
  raw native Promise settles. Connection or database loss may release that
  advisory lease earlier; a same-authorization cold retry is then safe only
  through the exact concurrent idempotent-or-fail-closed collector protocol and
  does not prove the older callback quiesced.
  On Linux, destructive collection resolves every artifact through a
  revalidated clone of the held state-root FD, including terminal lookup and
  final absence proof. Non-Linux collection retains pathname identity/policy
  brackets and does not claim resistance to an active same-UID ABA swap.
  Production deployment accepts only the exact process-local supervisor and
  collector pair returned by `createPodmanWriterSupervisorBundle()` before it
  constructs the physical adapter; matching IDs and owner strings alone are
  insufficient. Owner preparation and state/supervisor bundle construction
  fail closed before physical dispatch. Direct
  `createPodmanWriterSupervisor()` carries only a caller-asserted owner and is
  not a production deployment input. Migration 009 refuses to install while
  any legacy writer launch is `starting` or `uncertain`, or while any session
  retains a non-null current-launch pointer regardless of its shape or
  referential validity. The latter gate requires a committed current launch to
  be stopped or physically fenced and its current-launch pointer cleared before
  rollout. After installation, a deferred database constraint prevents an old
  binary from committing an ownerless dispatch. Only unbound `prepared` work remains
  owner-neutral for read/cancel cleanup. Historical unbound committed work that
  is no longer current receives no GC or adoption authority; its remaining
  terminal artifacts require explicit legacy cleanup.
  The marker prevents accidental routing but is neither cryptographic host
  attestation nor protection against an administrator cloning the root and
  marker together.
- One database-global versioned restore lifecycle lock now gives foreground
  composition a shared lease and the recovery runner an exclusive lease. The
  runner and guarded service calls revalidate the lease around lane, candidate,
  and cursor boundaries. A fixed-delay scheduler starts with one immediate bounded
  pass, prevents overlapping passes, coalesces concurrent kicks, reports busy
  or uncertain ticks through a synchronous observer, and drains admitted work
  during shutdown. The observer must return `undefined`; Promise and thenable
  returns are rejected to prevent observer/scheduler completion cycles. Cursor
  scopes do not partition this lock, and these components do not enable
  production restore.
- Stopped-directory backend contract version 3 can transport the complete
  authority-issued restore-generation binding to fresh publication or to
  source-free committed verification. Version 2 callback behavior remains
  compatible, and the new transport seam does not enable `runRestore()`.
- Detached-restore foreground phase A now provides a canonical caller-
  persisted root plan, invocation-time default-deny fleet admission, and one
  production-neutral facade under the shared lifecycle lease. The stable plan
  derives renewal, capture, generation, detach, activation, and launch IDs
  while keeping `captureCreatedAt` fixed across retry. The logical launcher and
  capture authority remain the source of the formal stop-operation and
  capture-attempt identities.
- The foreground order is renewal-before-stop, V3 stop/prepared capture,
  generation V1 publication, exact release or force-fence detach with no mode
  fallback, activation V2, and prepared launch. The standalone facade still
  depends on an exact stable resolver, while the assembled runtime now obtains
  that plan from its private durable read-only registry binding; neither adds
  an autonomous cross-stage saga. The plan's source checkpoint artefact path
  is distinct from the fresh safety-capture path selected by the capture
  backend from derived IDs.
  The assembled runtime fixes the configured state owner into this private
  foreground composition. Its launch-attempt read uses exact
  `{ operationId, stateOwnerId }`; public restore admission cannot choose or
  override that owner.
- The facade requires its nested per-operation guard pool to be distinct from
  both lifecycle pools before any connection is acquired. This prevents a
  max-one foreground lifecycle pool from self-deadlocking while its shared
  lease waits for a nested exclusive operation.
- A production-neutral runtime factory now constructs one internally
  consistent authority graph, private capture backend, private foreground
  composition, immutable public backend, and idle recovery scheduler from four
  pairwise-distinct borrowed pools. It performs no migration, start, stop,
  provider action, or pool close. Its backend is a low-level capability until
  claimed by the controller.
- The same factory now returns a narrow `writerLaunch` facet with only
  `runLaunch()` and `reconcileLaunchAttempt()`. Those receiver-preserving
  wrappers target the exact internal launcher already used by capture and
  foreground restore, so a same-process started writer retains its original
  opaque handle. Stop, retire, prepared-launch, handle-resolution, and internal
  launcher maps remain private; a committed row alone still cannot recover a
  cold handle.
- Migration 7 and the detached-restore stable-plan registry now durably bind
  one canonical admission and plan to the restore operation ID. Provisioning
  is separately fleet-gated and supports only immutable insert or exact
  replay; crossed identity fails closed and commit-acknowledgement loss is
  accepted only when exact durable readback proves the inserted plan.
  Resolution is read-only, checks the expected canonical session, and
  rehydrates the authentic plan capability without creating or repairing
  state. Generation dispatch revalidates that complete plan against the
  permanent claim digest and its exact generation and destination-isolation
  identities before it can grant publication authority.
- The production-neutral runtime constructs that registry from the same
  internal serializable store, exposes only the receiver-preserving
  `stablePlanProvisioning.provisionStablePlan()` wrapper, and passes the
  private receiver-preserving resolver to foreground execution. External
  callers cannot replace the foreground resolver or obtain it from the runtime
  surface.
- A deployment-owned runtime controller now uses that same store's narrow
  bootstrap facet. It completes migration and one exact full recovery sweep
  before opening checkpoint-backend, image-plan-reservation,
  plan-provisioning, or writer-launch admission. The raw foreground callback
  remains private. Shutdown closes those facets first, stops the scheduler,
  and drains every admitted call before the caller may close the four borrowed
  pools. The raw runtime remains a low-level capability and is not a parallel
  serving route.
- A higher PostgreSQL deployment factory now owns those four pools. It accepts
  only explicit connection, verified-TLS, timeout, role-capacity, and
  application-name configuration; constructs one private pool per authority,
  operation, foreground-lifecycle, and recovery-lifecycle role; and admits the
  controller startup only after simultaneous checked-out connections prove a
  writable PostgreSQL 13-or-newer database in one advisory-lock domain. That
  startup check is point-in-time evidence, not continuous primary-affinity
  monitoring. Stop drains the controller before attempting and awaiting all
  four pool closures; no pool or low-level runtime capability is exposed.
- Deployment now accepts one exact image-plan provider configuration and owns
  the private binding that maps an authentic plan's `imagePlanId` to exact OCI
  manifest/config bytes, trusted Codex inspection, and an opaque process-local
  reservation. Provider results settle only as exact frozen null-prototype
  records, closing inherited-`then` assimilation before binding validation.
  Its gated reservation facet exposes only preparation; the foreground and
  logical launcher share the binding for later revalidation.
  This is image identity authority only, not fetch, signature verification,
  container-runtime pinning or launch, supervisor/provider/storage execution,
  or a physical writer fence.
- The physical-collaborator settlement foundation now gives image-plan
  resolution and trusted Codex inspection separate explicit result deadlines
  and post-deadline settlement grace periods. Provider contract version 2
  receives one fresh opaque invocation identity and authentic abort signal per
  call. Deadline abort permanently rejects the result; grace only observes the
  original Promise, and failure to settle invokes a private deployment fatal
  hook. Neither abort nor that hook proves physical quiescence, makes attempted
  dependency cleanup a clean stopped result, or authorizes another dispatch.
- A deployment-private physical-binding graph now extends the same contract to
  three supervisor methods, a separate supervisor-state collector, nine
  storage-lifecycle methods, four publication methods, and restore-destination
  resolution. Together with image resolution and inspection, deployment owns
  twenty method-specific settlement stops.
  The raw Podman/physical supervisor, physical adapter facade, logical
  supervisor, logical reconcile receipt, collection surface/receipt, and
  aggregate binding are now versions 5, 4, 4, 2, 2, and 4 respectively. The raw
  launch receipt remains version 2, while the raw reconciliation receipt is now
  version 2; the durable launch request remains version 1 and owner-free.
  Supervisor and collector exact surfaces expose the same `stateOwnerId`.
  Production additionally
  requires their marker-backed process-local pair provenance; structural
  equality alone does not establish it.
  Transient invocation identities and abort signals remain outside durable
  requests/results; all existing grants, readbacks, stopped-only reconciliation,
  committed-only verification, and no-second-dispatch rules remain authoritative.
  Stop requests every settlement drain before awaiting them and closes pools
  only after controller and settlement drain; any failure is sticky.
- The completed assembled-restore safety matrix is version 2 and fixes the
  distinction between all twenty settlement contracts and the fifteen leaves
  on the private restore protocol surface: nine mutators, six observations,
  and five contract-only leaves. Seven one-shot dispatch mutators remain
  at-most-once per durable operation; terminal-state GC authorizes one exact
  chain whose
  acknowledgement-loss retry may prove it absent but cannot perform a second
  deletion. Six resolver, verifier, inspector, or observer-only reconciliation
  leaves may repeat across independent recovery attempts but cannot manufacture
  a grant. The five remaining generic lifecycle contracts
  stay outside the current saga. The evidence shape combines nine
  real-PostgreSQL durable-cut and commit-acknowledgement-loss paths, separate
  same-database/stable-plan retry through fresh physical bindings, image
  binding, runtime, and controller, stable-plan-registry rehydration, and
  representative settlement-foundation/deployment timer and drain cases. A
  test-only router separately locks the explicit fresh/committed publication
  callback seam. The immutable public backend now owns that routing in
  production without accepting a caller callback. The matrix does not claim
  one whole-saga deployment restart or operating-system crash coverage.
  The collector mutator remains
  `supervisorStateCollector.collectTerminalState`; its cut is
  `supervisor-state-gc`, durable key is
  `authorization.terminalOperationId`, and independent overlay is
  `supervisor-state-mutator`. The ninth mutator is the complete
  `supervisor.reconcileWriterLaunch` leaf, conservatively assigned to
  `supervisor-mutator`; its cut is `writer-launch-retirement` and its durable
  key is `attempt.launchAttemptId`.
- Restore activation now obtains and consumes its PostgreSQL one-shot dispatch
  grant entirely inside one coordinator-owned per-operation guard. The storage
  backend exposes a separate version 1 read-only reconciliation contract keyed
  by the durable activation request. `applied` can finalize exact existing
  evidence; `absent-and-quiescent` can reach the first attach only from that
  same guarded claim; `unknown`, retained `starting`/`uncertain` work, claim
  acknowledgement loss, and copied caller data remain blocked without a second
  physical dispatch. Durable activation request/result version 1 and the
  PostgreSQL schema remain unchanged.
- The final public checkpoint backend is complete. Runtime keeps the original
  capture backend private and constructs a second immutable backend whose
  restore authority is the version 3 foreground composition. Runtime,
  controller, and deployment expose only its checkpoint facade through
  ready/in-flight admission. Its metadata carries the same settled lifecycle
  capability tuple persisted on sessions and enforced by writer detach; raw
  lifecycle mutations, foreground callbacks, and operator/provider extensions
  stay private. Operational lease admission
  is enforced at stable-plan provision and every resolution from one
  deployment-owned policy derived from the two database-clock critical
  windows.
  No published path, generation row, serialized measurement, attempt record,
  or discovery result is writer-launch authority by itself.
- Production-injectable Linux ext4 physical components now supply sparse raw-
  image lifecycle below host-owned `rprivate` mount carriers, exact close-
  before-unmount and loop-detach settlement, an automatically rotated
  provider-state checkpoint and bounded active delta log checked against an
  external PostgreSQL head, two distinct persistent archive control identities,
  and a rootless digest-pinned Podman writer supervisor. Migrations 010 and 011
  make PostgreSQL the permanent exact-operation replay index for provider-state
  version 3. The local version 3 checkpoint keeps all current storage records,
  destroyed tombstones, attachment-origin operation IDs, and only the live
  prepared recovery working set; committed history is no longer copied across
  rotation. The contract-version-3 runtime authority validates one compact
  projection request in a single serializable transaction: it independently
  streams and fully normalizes the complete prepared set through one data
  `SELECT` whose `LIMIT` comes from the exact stored head's structural bound.
  It validates `A` committed attachment origins in independent fixed 65,535-ID
  input/query batches through `max(1, ceil(A / 65,535))` streamed origin data
  `SELECT` statements, in addition to the exact-head `SELECT`. SQL parameters
  and additional memory remain bounded per batch, and a domain-separated
  receipt is returned only for an exact match. The provider reuses that receipt
  only for the same authority instance, exact head, and unchanged loaded
  generation; uncertain acknowledgement and adoption paths require fresh validation.
  A provider-locked version 2 adoption replays revisions `1..N`, proves storage
  lineage and final projection, durably writes the covering version 3 files,
  and then imports or verifies the complete PostgreSQL history in the same
  serializable cut as the version 3 head and completeness marker. Migration
  011's database-supplied transaction ID and deferred coverage trigger reject
  head-only, partial, duplicate, extra, or cross-manifest imports. The manifest
  is an acknowledgement receipt rather than a write token. Native commits use
  `indexed-frame-v1`; only that adoption transaction can write
  `unavailable-adopted-v2`. Commit acknowledgement loss is resolved by exact
  head, marker, manifest, and full-row readback. Migration 011 is unchanged by
  the adoption transport expansion. The original contract version 1 authority
  still consumes exact full arrays. The explicit contract version 2 authority
  consumes restartable operation and storage cursor pages with fixed four-item
  page work, stages and replays them through `pg_temp` relations created with
  `ON COMMIT DROP` in the same `SERIALIZABLE` transaction, and excludes pager
  versions, cursors, and page boundaries from the manifest. Every partition of
  the same canonical state therefore retains the exact version 1 manifest bytes
  and the same durable candidate, `pending`, acknowledgement-loss, and
  `verified` recovery semantics.
  Migration 011 also claims each prepared and committed revision in an
  internal unique event registry. It validates every migrated non-null marker
  once, then permits only a one-revision indexed append with its deferred event
  claim or a revision-preserving rotation; bulk adoption remains subject to
  the separate full-range proof. A raw head insert or jump therefore cannot
  assert completeness without permanent history. Stored heads exclude
  revision zero, and a database-managed progress transaction ID prevents an
  early constraint check from authorizing a second head mutation.
  The latest validated committed operation per storage remains the PostgreSQL
  projection, so a complete canonical `storageStateBefore` mismatch rolls the
  head, marker, and history back together. Destroyed storage remains a durable
  tombstone. Version 3 `inspectCapacity()` counts only live prepared operations
  as retained local operations. Full-array adoption version 1 is deliberately
  capped at 65,535 operations, 65,535 storages, and 64 MiB of aggregate
  canonical operation/prepared-projection/storage material; those limits apply
  only to that adoption transport, which continues to fail an oversized request
  with `state_capacity_exhausted` before candidate mutation. Paged adoption
  version 2 removes those version 1 full-array transport limits without changing
  the 4 MiB record/frame-payload bound, the active-tail 65,535-frame/64 MiB
  envelope, uint32 checkpoint-count limits, or legal version 3 runtime-
  projection bounds. A
  permanent anchor-lifecycle row serializes every head insert through the same
  unique key; full teardown of any operation history moves that row from active
  to immutable retired, so
  neither an early deferred-constraint check nor a concurrent transaction can
  recycle the same provider/anchor identity. Every head deletion retires the
  row, including an empty head, to prevent durable-anchor ABA. Producer
  outputs bind
  the archive mount-root and artifact-child tuples separately; on the consumer,
  the former makes the first remount verification-only and the latter
  authorizes verification for the exact publication root. Two hosted Ubuntu
  runners cover clean detach, transfer, remount, provider-head continuity, and
  source-free committed verification; the producer additionally gates private-
  namespace non-propagation. This production path remains a clean/manual-
  fencing boundary: it exposes no crash-prefix checkpoint API, does not prove
  sudden power loss or automatically fence a stale writer, and does not
  implement differential export/
  compression, encryption, production history offload, or registry trust. The
  initialized ext4
  backend now binds committed attachment identity into Podman filesystem
  authority in the same non-root producer process. Provider persistent
  filesystem/file-handle identity and the driver's same-sample runtime
  `device`/`inode` are matched to Podman's held FD and live bind; access policy
  is checked separately, while child-entry and content churn remain allowed.
  Within the default authority boundary, parent-held directory FDs are matched
  to a temporary holder in Podman's rootless namespace, create must preserve
  that exact procfd source in Podman's external `created` state before start,
  a new create must return its complete 64-hex container identity, and image
  `Config.User` drives the explicit non-root `keep-id` UID/GID mapping used by
  the conformance writer. Ordinary failures observed before child exit request
  whole-group termination; every
  failure waits for direct close and kernel-proved group absence without ever
  signalling the frozen PGID after exit. Holder shutdown separately requires
  wrapper close plus group absence; a dispatched exact start only advances on
  a zero exit and otherwise remains pending because conmon may leave the CLI
  group.
- A separate root-only Ubuntu conformance job now kills and joins one writer
  after it has fsynced complete plain-JSONL records and one synthetic partial
  suffix plus its rollout directory on ext4, then atomically snapshots the
  mounted origin through LVM/device-mapper. The harness does not independently
  verify a whole-filesystem freeze/flush; its durable fixture evidence is
  limited to the explicitly fsynced rollout bytes and directory entry.
  It exports the snapshot as a fixed raw artifact, repairs only an independent
  writable raw copy, proves no abort marker was invented, and appends and
  rereads one synced valid event. This
  evidence does not simulate sudden power loss or storage-controller cache
  loss, fence a stale writer, emit a checkpoint descriptor, or widen the
  production backend's `atomicPointInTimeCheckpoint: false` capability.
- A second root-only Ubuntu job now provides external-QEMU-SIGKILL sudden guest
  power-loss evidence. Setup, armed, and recovery boots use the same
  `data.raw` object. The armed guest fsyncs one exact 4,096-byte valid JSONL
  prefix and its rollout directory entry, writes an unsynced synthetic tail,
  and remains live until the external host controller sends `SIGKILL` to and
  joins the exact non-daemonised QEMU child. Host-side recovery admission
  accepts zero through the attempted tail length of arbitrary bytes only when
  they contain no LF, complete JSON value, or abort marker; it preserves the
  prefix and uses the production repair primitive to converge the rollout
  exactly back to that prefix before a durable continuation. The host runner
  and storage stay
  online; QMP verifies only the configured `writeback=true`, `direct=true`,
  `no-flush=false` tuple. This is not host, controller, or drive cache-loss
  evidence and supplies no production checkpoint or fencing authority.
- Terminal local Podman supervisor state now has a bounded two-phase collector.
  It validates the exact stopped revision 4 terminal record and revisions 0
  through 3 or their admitted oldest-first missing retry prefix, removes the
  lower revisions plus publication sidecars and syncs
  the held directory, then compares revision 4's named object and bytes with
  its held FD before unlink. After unlink it positionally rereads exact bytes
  through that FD, revalidates identity and access policy, proves absence, and
  syncs the directory again. Object identity (`dev`/`ino` plus held FDs),
  content stability, and access policy remain separate protected properties.
  That policy checks same-UID regular files at `0600` with required `nlink`,
  the same-UID state root and immediate parent at `0700`, and safe traversal-
  ancestor ownership/write/sticky constraints; child-entry and generic `stat`
  churn are not change evidence.
  I/O or unreadable state, canonical conflict, and post-mutation outcome
  uncertainty remain distinct. PostgreSQL finalization and outer lifecycle/
  settlement provide authority and normal-path quiescence while the owning
  session remains live; same-authorization monotonic collection handles the
  explicitly non-quiescent session-loss overlap. The collector does not mint
  either outer property.
- Cold reconciliation physically retires a container only from an exact durable
  stopped revision 4 record. It completes idempotent `podman rm --ignore`, then
  proves exact anchored-name and exact-ID absence with two independent
  `podman ps -a --no-trunc` queries before returning that record to the
  owner-bound GC finalizer. Ambiguous removal, either absence query, physical
  adaptation, or a pre-commit finalizer failure preserves revision 4 and
  commits no database finalization. A post-COMMIT acknowledgement loss may
  instead follow an atomic commit of the operation and owner-bound GC
  authorization; exact authorization readback determines whether that commit
  exists. Revision 4 remains until the authorized collector removes it in
  either case. Observer-only `complete-stopped` and `not-started` return a null
  terminal record and retain the legacy no-GC path.
- Per-workstream implementation state lives under `docs/project_journal/`.

## Recovery Pointers

- Runtime delivery plan:
  `docs/project_journal/2026/07/2026-07-01-runtime-delivery-plan-6f13a8.md`
- Provider-state operation index:
  `docs/project_journal/2026/08/2026-08-20-provider-state-operation-index-c7e4a1.md`
- Provider-state version 3 adoption:
  `docs/project_journal/2026/08/2026-08-20-provider-state-v3-adoption-9d4c2e.md`
- Provider-state adoption capacity:
  `docs/project_journal/2026/08/2026-08-28-provider-state-adoption-capacity-7b6d41.md`
- Linux ext4 physical backend:
  `docs/project_journal/2026/08/2026-08-14-linux-ext4-physical-backend-7c4e91.md`
- LVM crash-prefix conformance:
  `docs/project_journal/2026/08/2026-08-28-lvm-crash-prefix-conformance-d6a3f2.md`
- QEMU sudden guest power-loss conformance:
  `docs/project_journal/2026/08/2026-08-28-qemu-guest-power-loss-conformance-e8b4c1.md`
- ext4-to-Podman attachment composition:
  `docs/project_journal/2026/08/2026-08-19-ext4-podman-composition-a4c821.md`
- Terminal writer-supervisor state GC:
  `docs/project_journal/2026/08/2026-08-19-writer-supervisor-state-gc-b7d4e2.md`
- Final public restore backend:
  `docs/project_journal/2026/08/2026-08-13-final-public-restore-backend-4b7c2e.md`
- Assembled restore safety matrix:
  `docs/project_journal/2026/08/2026-08-12-assembled-restore-safety-matrix-6d3a91.md`
- Physical-collaborator settlement:
  `docs/project_journal/2026/08/2026-08-12-physical-collaborator-settlement-a3f9c2.md`
- Physical settlement graph:
  `docs/project_journal/2026/08/2026-08-12-physical-settlement-graph-f4c8a1.md`
- Restore-activation reconciliation:
  `docs/project_journal/2026/08/2026-08-12-restore-activation-reconciliation-b6d4e1.md`
- Detached-restore image-plan binding:
  `docs/project_journal/2026/08/2026-08-12-detached-restore-image-plan-binding-e7b3c9.md`
- Detached-restore stable-plan registry:
  `docs/project_journal/2026/08/2026-08-11-detached-restore-stable-plan-registry-8e4c21.md`
- Auth refresh authority spike:
  `docs/project_journal/2026/07/2026-07-01-auth-refresh-authority-8b2e41.md`
- Interrupted-turn recovery spike:
  `docs/project_journal/2026/07/2026-07-01-interrupted-turn-recovery-4a91c7.md`
- Pinned-executable resume and rollout-tail repair:
  `docs/project_journal/2026/07/2026-07-15-pinned-executable-resume-tail-repair-9d813d.md`
- Session filesystem and storage contracts:
  `docs/project_journal/2026/07/2026-07-02-session-storage-contracts-7c31e2.md`
- Auth broker MVP:
  `docs/project_journal/2026/07/2026-07-02-auth-broker-mvp-4d729b.md`
- Snapshot and restore core:
  `docs/project_journal/2026/07/2026-07-02-snapshot-restore-core-3e8a71.md`
- Reusable stopped-tree primitives:
  `docs/project_journal/2026/07/2026-07-02-stopped-tree-primitives-6d83af.md`
- Durable filesystem operation journal:
  `docs/project_journal/2026/07/2026-07-02-filesystem-operation-journal-2f6c91.md`
- Stopped-directory publication:
  `docs/project_journal/2026/07/2026-07-02-stopped-directory-publication-7a4c2e.md`
- Same-process stopped-writer capability:
  `docs/project_journal/2026/07/2026-07-14-stopped-writer-capability-8c4e2a.md`
- Stopped-directory backend:
  `docs/project_journal/2026/07/2026-07-14-stopped-directory-backend-c5a91e.md`
- Committed capture reconciliation:
  `docs/project_journal/2026/07/2026-07-14-capture-reconciliation-91eac4.md`
- Session authority foundation:
  `docs/project_journal/2026/07/2026-07-23-session-authority-foundation-b7419e.md`
- Canonical session registry:
  `docs/project_journal/2026/07/2026-07-29-canonical-session-registry-4e8a2d.md`
- Durable operation and reservation kernel:
  `docs/project_journal/2026/07/2026-07-29-operation-reservation-kernel-f3c8a1.md`
- Writer lease and attachment acquisition:
  `docs/project_journal/2026/07/2026-07-30-writer-lease-attachment-7b3e92.md`
- Writer release and force-fence reconciliation:
  `docs/project_journal/2026/07/2026-07-31-release-force-fence-e4b9c7.md`
- Production checkpoint mutation authority:
  `docs/project_journal/2026/07/2026-07-31-checkpoint-mutation-authority-c3a8f2.md`
- Bounded checkpoint recovery service:
  `docs/project_journal/2026/08/2026-08-02-checkpoint-recovery-service-7d2c4a.md`
- Restore-generation schema foundation:
  `docs/project_journal/2026/08/2026-08-03-restore-generation-schema-c0a7f4.md`
- Typed restore-generation authority:
  `docs/project_journal/2026/08/2026-08-04-restore-generation-authority-a4d912.md`
- Durable launch-attempt lifecycle:
  `docs/project_journal/2026/08/2026-08-04-durable-launch-attempt-lifecycle-6e2f8b.md`
- Logical writer launcher foundation:
  `docs/project_journal/2026/08/2026-08-04-logical-writer-launcher-foundation-b6d3e1.md`
- Atomic restore-to-launch handoff:
  `docs/project_journal/2026/08/2026-08-04-restore-launch-handoff-5a7c2e.md`
- Restore publication attachment-contract correction:
  `docs/project_journal/2026/08/2026-08-05-restore-publication-attachment-contract-4c7a91.md`
- Restore-generation full publication binding:
  `docs/project_journal/2026/08/2026-08-10-restore-generation-publication-binding-b91f4a.md`
- Detached restore activation and recovery composition:
  `docs/project_journal/2026/08/2026-08-05-detached-restore-activation-3f91c2.md`
- Capture-bound restore activation compatibility:
  `docs/project_journal/2026/08/2026-08-06-capture-bound-restore-activation-c4a2d8.md`
- Activation launch compatibility:
  `docs/project_journal/2026/08/2026-08-10-activation-launch-compatibility-a6e4c2.md`
- Durable restore recovery cursors and bounded runner:
  `docs/project_journal/2026/08/2026-08-10-restore-recovery-cursors-5d82a1.md`
- Durable stop-to-clean-capture composition:
  `docs/project_journal/2026/08/2026-08-05-durable-stop-capture-composition-7e3a91.md`
- Durable stop-to-prepared-capture handoff:
  `docs/project_journal/2026/08/2026-08-10-durable-stop-prepared-capture-handoff-d4c6a1.md`
- Detached restore foreground composition:
  `docs/project_journal/2026/08/2026-08-11-detached-restore-foreground-composition-4f8c2d.md`
- Production-neutral restore runtime assembly:
  `docs/project_journal/2026/08/2026-08-11-production-neutral-restore-runtime-assembly-8b42f1.md`
- Restore runtime writer-launch ingress:
  `docs/project_journal/2026/08/2026-08-11-restore-runtime-writer-launch-ingress-c83e71.md`
- External-auth probe workstream:
  `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`

## Global Blockers

- The app-server `chatgptAuthTokens` integration is experimental and requires a
  pinned Codex binary or image plus compatibility testing on upgrades.
- Codex rollout flush is not a stable-storage sync barrier. The implemented
  repair covers only pinned plain-JSONL tail framing on a detached restored
  copy; production recovery still needs external sync/freeze, atomic crash
  capture, trusted OCI resolution, fencing, and launcher admission.
- Restore is now available through the deployment-controlled public checkpoint
  backend. Capture-bound detached activation and bounded no-relaunch recovery
  exist, the stop-to-prepared-capture handoff is durable, and the cross-
  process lifecycle guard, scheduler, stable root plan, invocation-time fleet
  gate, foreground composition seam, production-neutral runtime assembly, and
  same-launcher writer-start ingress are implemented. The durable stable-plan
  registry, separately gated provisioning surface, and private read-only
  foreground resolver are now implemented as well. Migration-before-serving,
  the initial recovery sweep, controlled restore admission, scheduler stop,
  admitted-call drain, explicit PostgreSQL bootstrap configuration, and pool
  closure now have one deployment owner; its `stop` facet is not distributed
  to injected runtime collaborators. Its image-plan binding owns exact OCI
  bytes, trusted inspection, opaque reservation identity, and separate bounded
  settlement policy for provider resolution and inspection. The same owner now
  binds every assembled supervisor, terminal-state collector,
  storage-lifecycle, publication, and restore-destination resolver Promise
  into the common settlement lifecycle.
  The deployment now also derives and admits one exact operational lease
  policy across the two database-clock windows, including physical deadline/
  grace bounds, an aggregate database allowance, and a safety margin.
  The scoped assembled safety matrix and immutable public adapter assembly are
  complete. Production-injectable Linux ext4 and rootless Podman components
  now implement the clean/manual-fencing physical boundary and verify clean
  two-host detach, transfer, and a verification-only first remount against
  separately anchored archive control tuples. Each ext4 job runs below
  dedicated `rprivate` carriers in one long-lived private mount namespace and
  the producer gates whether its live mounts propagate to the parent
  namespace. The producer now invokes the initialized ext4-to-Podman
  composition after attach and before detach, launches a real rootless Podman
  writer in that same non-root process, and carries its marker through the
  clean transfer for consumer verification. Before detach, the dedicated
  producer proves its complete rootless container and pod inventories empty,
  retires the user-wide Podman pause namespace, and relies on the native loop
  receipt—not container stop alone—for physical quiescence. This release step
  is forbidden on a shared UID or Podman engine. The independent Podman job
  remains as narrower supervisor coverage. Those production jobs expose no
  crash-prefix checkpoint API, revoke no stale remote writer automatically,
  and supply no
  differential/compressed export, encryption, provider-state retention, or
  registry trust. A separate root-only LVM job now covers the narrower stopped-
  writer, explicitly fsynced rollout, atomic block snapshot, and detached-
  writable-copy tail-repair sequence. It makes no independently verified
  whole-filesystem freeze/flush claim, is not sudden-power-loss or controller-
  cache-loss evidence, and does not change production capabilities. A second
  evidence-only job cold-boots the same raw ext4 object after an external host
  controller sends `SIGKILL` to its exact non-daemonised QEMU child. This
  proves the configured sudden guest power-loss boundary while the host and
  storage remain online; it does not prove host, controller, or drive cache
  loss and changes no production API or capability. Terminal
  supervisor-state GC safety after PostgreSQL terminal
  commit is complete for both healthy-session callback quiescence and exact
  same-authorization session-loss overlap. The provider operation-index
  authority, atomic version 2-to-3 adoption, current attachment-origin binding,
  committed-history removal from local checkpoints, and restartable paged
  adoption beyond the full-array version 1 transport limits are complete.
  Remaining Linux/provider follow-up stays in the already-listed, separately
  scoped host/controller/drive cache-loss evidence, production crash-capture,
  automatic-fencing, and distribution work.
