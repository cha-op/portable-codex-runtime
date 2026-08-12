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
  without invoking the provider. The facade remains unscheduled and production
  restore remains fail-closed.
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
  attempts, and current-launch inventory through four independent keyset
  cursors. Recovery is source-free and no-relaunch: it never republishes,
  reserves or consumes an image, invokes a launcher, or reconstructs an opaque
  writer capability. Current-launch handling is inventory only.
- PostgreSQL now persists those four cursors independently for each configured
  recovery scope. A bounded single-flight runner advances each settled lane by
  revision/cycle compare-and-swap, survives restart and commit-acknowledgement
  loss, and preserves earlier lane progress when a later lane fails.
- One database-global versioned restore lifecycle lock now gives foreground
  composition a shared lease and the recovery runner an exclusive lease. The
  runner and service revalidate the lease around lane, candidate, and cursor
  boundaries. A fixed-delay scheduler starts with one immediate bounded
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
- The facade requires its nested per-operation guard pool to be distinct from
  both lifecycle pools before any connection is acquired. This prevents a
  max-one foreground lifecycle pool from self-deadlocking while its shared
  lease waits for a nested exclusive operation.
- A production-neutral runtime factory now constructs one internally
  consistent authority graph, capture-only backend, standalone foreground
  facade, and idle recovery scheduler from four pairwise-distinct borrowed
  pools. It performs no migration, start, stop, provider action, or pool close,
  and it does not inject foreground restore into the checkpoint backend.
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
  before opening foreground, image-plan-reservation, plan-provisioning, or
  writer-launch admission. Shutdown closes those facets first, stops the
  scheduler, and drains every admitted call before the caller may close the
  four borrowed pools. The raw runtime remains a low-level capability and is
  not a parallel serving route.
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
- The production checkpoint adapter remains capture-only. Restore fails closed
  because its `runRestore()` stub is unchanged. Production enablement still
  requires bounded physical-collaborator settlement/deadline policy,
  operational lease admission, full assembled restart and ambiguous-outcome
  validation, and the final public adapter route.
  No published path, generation row, serialized measurement, attempt record,
  or discovery result is writer-launch authority by itself.
- Per-workstream implementation state lives under `docs/project_journal/`.

## Recovery Pointers

- Runtime delivery plan:
  `docs/project_journal/2026/07/2026-07-01-runtime-delivery-plan-6f13a8.md`
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
- Restore remains intentionally unavailable in the production checkpoint
  adapter. Capture-bound detached activation and bounded no-relaunch recovery
  now exist, the stop-to-prepared-capture handoff is durable, and the cross-
  process lifecycle guard, scheduler, stable root plan, invocation-time fleet
  gate, foreground composition seam, production-neutral runtime assembly, and
  same-launcher writer-start ingress are implemented. The durable stable-plan
  registry, separately gated provisioning surface, and private read-only
  foreground resolver are now implemented as well. Migration-before-serving,
  the initial recovery sweep, controlled restore admission, scheduler stop,
  admitted-call drain, explicit PostgreSQL bootstrap configuration, and pool
  closure now have one deployment owner; its `stop` facet is not distributed
  to injected runtime collaborators. Its image-plan binding now owns exact OCI
  bytes, trusted inspection, and opaque reservation identity. Production still
  requires bounded physical-collaborator settlement and deadlines, operational
  lease admission, whole-graph fail-closed validation, and final public-adapter
  assembly before `runRestore()` may be enabled.
