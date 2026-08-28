# Runtime Delivery Plan

## Objective

Build a portable Codex app-server runtime whose session filesystem can move
between trusted hosts while authentication remains a separate monotonic control
plane.

## Delivery Invariants

- Deliver one pull request at a time.
- After each squash merge, update local `master` and create the next branch from
  that new commit.
- Run the complete repository test suite for every pull request.
- Require one fresh-context local Codex review and one exact current-head
  `@codex review` request before merge. This repository does not require a
  Claude lane.
- Evaluate the GitHub lane under the active trusted review-evidence policy.
  Task-local evidence overrides belong to the delivery orchestration record;
  they are not a permanent repository completion contract.
- Resolve every pull-request conversation before merge.
- Keep credentials, untracked private files, and unrelated repositories outside
  review and evidence artifacts.
- Serialize live tests that use `.test-codex-home`; copied refresh-token state is
  not an independent test credential.

## Pull Request Sequence

1. **PR #1: external-auth compatibility probe**
   - Prove the pinned app-server's external access-token injection and refresh
     callback behaviour without persisting worker `auth.json`.
2. **PR #2: auth refresh authority spike**
   - Prove a central authority can refresh ChatGPT credentials without a normal
     model turn and preserve redacted evidence.
3. **PR #3: interrupted and killed turn recovery spike**
   - Record Codex behaviour for logical interruption, process signals, hard
     kills, and stopped-tree recovery.
4. **PR #4: session filesystem and storage contracts**
   - Define the session manifest, normal-directory attachment, lease, fencing,
     backend mutation, and checkpoint descriptor contracts.
5. **PR #5: auth broker MVP**
   - Implement encrypted canonical auth state, single-flight refresh,
     generation/CAS semantics, and worker token delivery.
6. **PR #6: snapshot and restore core**
   - Implement backend-neutral stopped-writer clean checkpoint orchestration
     with exact descriptor/result validation and fail-closed uncertainty.
7. **PR #7: reusable stopped-tree primitives**
   - Extract the validated copy, digest, mount, ACL, pathname, and guarded
     cleanup layer without claiming durable publication.
8. **PR #8: durable filesystem operation journal**
   - Persist exact prepared, materialized, and committed operation records and
     predetermined results with canonical replay after restart.
9. **PR #9: stopped-directory publication layer**
   - Bind journal phases to a local filesystem storage barrier, deterministic
     private staging, atomic checkpoint-bundle or restore-tree publication,
     exact readback, and pre-commit consumer isolation.
10. **PR #10: same-process stopped-writer capability**
    - Convert one trusted, fully joined writer stop into one same-process,
      one-use object capability bound to the exact process and writer
      incarnations, attachment, fence, and stop operation; consume it around
      one snapshot callback without embedding stop mechanics in storage.
11. **PR #11: stopped-directory backend adapter**
    - Compose the journal, publication layer, capability, and snapshot core;
      add a durable mutation-authority/catalogue seam and atomic fresh-capture
      admission; then run the complete backend conformance and
      failure-injection matrix.
12. **PR #12: committed capture reconciliation**
    - Bind normal capture to authenticated durable attempt provenance and add a
      source-free, committed-only reconciliation path that cannot advance
      `prepared` or `materialized` publication state.
13. **PR #13: pinned-executable rollout-tail repair**
    - Repair a missing final LF or one invalid unterminated tail on a detached
      writable copy, bind every rollout to the audited writer version, and
      preserve six-scenario same-pinned-executable recovery evidence.
14. **PR #14: session authority foundation**
    - Add a same-client PostgreSQL serializable executor, checksum-bound
      authority schema, real concurrency tests, and a bounded OCI/Docker
      runnable-image reservation without claiming writer lifecycle or
      container launch.

The sequence through PR #14 plus the first six follow-on slices is complete.
The six-item follow-on sequence does not preallocate GitHub PR numbers:

1. **Canonical session registry (complete)**
   - Register one immutable manifest, storage reference, and backend capability
     set per session; add strict canonical readback and serializable
     concurrent-replay coverage without authorizing a writer.
2. **Durable operation and reservation kernel (complete)**
   - Claim canonical operation IDs, reserve conflicting mutations, and retain
     uncertain outcomes for explicit reconciliation.
3. **Writer lease and attachment acquisition (complete)**
   - Reuse the existing schema and JSONB documents without DDL. After generic
     reservation, atomically allocate a bounded database-clock lease,
     deterministic lease and attachment IDs, and the next uint64 fencing epoch
     while claiming typed dispatch and entering `ATTACHING`.
   - Keep the provider outside the transaction, then atomically finalize exact
     mutation and attachment evidence to `ATTACHED` from `starting` or
     `uncertain`, even if the lease expired while the provider ran.
   - Renew only `expiresAt` in one database-clock, globally operation-ID
     idempotent terminal transaction; expiry equality cannot be renewed.
4. **Release and force-fence reconciliation (complete)**
   - Reuse the existing schema without DDL. Add `writer-release-v1` and
     `writer-force-fence-v1` to the generic reserve, typed dispatch,
     provider-outside-transaction, and typed finalize protocol.
   - Let only the exact owner detach the unchanged attachment after expiry
     without advancing the epoch. Finalize ambiguous acquire, release, or
     force-fence outcomes to `BLOCKED` while retaining their tuple, target, and
     current epoch.
   - Advance the uint64 epoch only when force-fence dispatch definitely commits
     from `ATTACHED` or `BLOCKED`, then enter `FENCING`. Enter `DETACHED` only
     from an independently validated dedicated force-fence proof; unavailable
     fencing returns to `BLOCKED` with the advanced epoch retained.
   - Keep manual backends incapable of completing the automatic proof. Lease
     expiry and database epoch state remain logical admission evidence, never a
     physical fence.
5. **Production checkpoint mutation authority and catalogue (complete)**
   - Reuse the version 1 schema without DDL. Bind one exact durable clean
     capture operation and reservation to the canonical stopped-writer
     admission, globally unique capture-attempt claim, predetermined result,
     and checkpoint-catalogue finalization.
   - Keep publication outside database transactions while a per-operation
     PostgreSQL session advisory guard and the active session reservation
     serialize each live capture or reconciliation invocation. Treat the
     durable operation, claim, and reservation—not an advisory lock
     reacquisition—as the restart blocker; recovery remains source-free and
     read-only until the physical journal proves exact commit.
   - Retain claims permanently, reject pre-existing tombstones, and reconcile
     only an exact already committed artefact without a source, writer, lease,
     attachment, clock, or stopped-writer capability.
   - Keep the private checkpoint-mutation adapter capture-only. Restore stays
     outside this adapter; the later completed public checkpoint facade binds
     one canonical detached destination generation to launcher admission.
6. **Bounded checkpoint recovery service (complete)**
   - Enumerate retained `starting` or `uncertain` capture operations, plus only
     the exact materialized V3 handoff operations still in `prepared`, in
     bounded `session_id` keyset pages and reconstruct their durable
     admissions. Prepared work may take its one fresh dispatch; active work
     invokes only committed, source-free reconciliation.
   - Process one page sequentially, advance the cursor only after each item
     settles, drain in-flight work on abort, and leave guard-busy or
     unverifiable operations durably blocked for a later pass.

Restore and launcher authority are now split into eight serial pull requests:

1. **Restore-generation schema foundation (complete)**
   - Replace the single hard-coded migration with one ordered,
     checksum-bound migration chain whose installed ledger must be an exact
     contiguous prefix.
   - Add a permanent `restore_destination_generations` relation with separate
     generation and operation identities, cross-session-safe foreign keys, and
     fail-closed authorized/committed row shapes.
   - Do not claim, finalize, or authorize a generation in this slice.
2. **Typed restore-generation authority (complete)**
   - Claim one canonical destination generation from an exact committed
     checkpoint, bind the restore fence and isolated destination, and finalize
     the predetermined restore result through typed PostgreSQL transitions.
   - Keep publication outside transactions and keep production restore
     fail-closed until launcher composition is complete.
3. **Durable launch-attempt lifecycle (complete)**
   - Reserve and claim one exact launch attempt, persist every
     `starting` or `uncertain` blocker, and accept only exact supervisor
     evidence when finalizing a started or stopped attempt.
   - Bind the finalized destination generation, session attachment, lease,
     epoch, image measurement, process incarnation, and writer incarnation
     without invoking a launcher in this slice.
   - Reuse the permanent operation and reservation rows with the operation ID
     as the launch-attempt ID; launch phase state needs no separate relation.
   - Upgrade canonical session documents to version 3 on the next real write
     so a started launch remains linked to its immutable operation after
     `lastOperation` advances.
4. **Logical launcher foundation (complete)**
   - Add a hardened PostgreSQL launcher facade that revalidates the original
     image reservation, persists `starting`, consumes that reservation once,
     invokes one external launch callback, and registers a provisional writer
     before finalising a started attempt.
   - Reconcile active attempts through supervisor evidence without relaunch,
     add typed `writer-launch-stop-v1` authority, and enumerate bounded
     prepared attempts and current launches without enabling production
     restore or claiming production stop/capture composition.
5. **Atomic restore-to-launch handoff (complete)**
   - Add a version 2 restore-generation request that commits to the exact
     measured image, supervisor, and launch-attempt identity before physical
     publication begins.
   - Add migration version 3's permanent global operation-ID registry. Direct
     operations materialize their claim immediately; version 2 restore
     dispatch durably claims the exact launch-attempt ID before authorizing
     publication, so no other session or operation can steal it in the crash
     gap. Drain in-flight legacy writes by locking the session table before the
     operation table, matching runtime lock order, and retain a database
     trigger that blocks old binaries from advancing V2 work without the exact
     claim while still allowing strict cancellation before dispatch.
   - In one serializable transition, commit the authorized generation, retire
     the restore operation, materialize its exact registry claim as the
     `writer-launch-attempt-v1` operation, and reserve that operation. Advance
     the canonical session first to the restore terminal anchor and then to
     the prepared launch pointer.
   - Prepare the process-local image reservation before publication and let
     the launcher claim only that already-reserved attempt. A crash cannot
     expose a committed generation without durable launch work.
   - Close expiry retirement over the same serialized launch boundary. After
     locking the session and active operation/reservation rows and proving the
     committed, immutable version 2 terminal restore, generation, and
     materialized registry provenance, permit only exact reason
     `launch-dispatch-not-started` when `expiresAt <= authorityNow`. Persist
     that sampled authority time on the operation, released reservation, and
     terminal session while retaining the registry claim permanently.
     Dispatch claim keeps the complementary `expiresAt > authorityNow`
     boundary; wrong reasons and pre-expiry cancellation reject.
   - Treat an expired cancellation as a valid terminal successor when the
     original atomic handoff API replays. Preserve ordinary version 1
     cancellation and replay behavior unchanged.
   - Preserve version 1 behavior, but fail the schema upgrade closed if an old
     version 2 restore operation has progressed beyond `prepared`: only an
     undispatched request can safely receive the missing pre-publication
     registry claim.
6. **Durable stop and capture composition (complete)**
   - Default-deny fresh restore-generation request version 2 creation until
     startup confirms fleet-wide authority and recovery compatibility. Exact
     existing replay remains available when the rollout decision later closes.
   - Prepare one exact clean-capture tuple, derive the stop operation from the
     complete tuple and current launch attempt, route one physical stop through
     `writer-launch-stop-v1`, and validate the committed operation,
     reservation, session, and complete-stop proof before issuing one opaque
     capability. Retain the local writer identity until capture succeeds;
     reconcile only the exact same-process `prepared` or explicitly granted
     `starting` pre-dispatch state, accept lease-expiration extension only for
     the same stable writer-fence identity, and block same-session successor
     launch in the canonical local coordinator until retirement. Finalize from
     exact `starting` or authority-proven `uncertain` without repeating physical
     stop. Ambiguous stop, finalisation, capture, or retirement stays
     fail-closed.
7. **Detached restore activation and recovery composition (complete)**
   - Keep the absent restore-publication destination independent from the
     current active attachment. After exact committed publication, obtain
     provider-backed attachment evidence for that object and atomically
     replace the canonical session attachment while materializing the exact
     prepared launch request.
   - Require the old attachment to be stopped, cleanly captured, fenced, and
     detached before activation. Historical request version 1 keeps its exact
     replay relation; request version 2 binds the committed stop, clean capture,
     and later detach without equating the stopped writer's generation with the
     target restore generation. Pathname equality is correlation only and never
     proves attachment authority or filesystem object identity.
   - Preserve the historical activation-v2 backward topology from detach to
     capture to stop. Also admit a fresh topology from detach to a committed
     version 1 target generation, then capture and stop, so generation creation
     may occur after clean capture and before detach without rewriting old
     durable work.
   - Verify committed restore publication against the detached destination
     and activated attachment before any prepared launch can proceed.
   - Add source-free committed-only destination verification and the optional
     version 1 provider activation extension. Bind physical object identity,
     content digest, access policy, attach mutation, and proof echoes without
     treating path equality as attachment authority.
   - Independently default-deny fresh restore-generation-v2 and capture-bound
     activation-v2 creation until startup confirms matching fleet
     compatibility. Keep the generation-predecessor activation-v2 topology
     behind its own default-closed
     `restoreAttachmentActivationV2GenerationPredecessorFleetCompatible`
     decision; the old topology and exact durable replay bypass only that
     fresh-topology backstop.
   - Claim and finalize versioned `restore-attachment-activation-v1` requests
     through serializable authority transitions. The final transition installs
     the exact canonical attachment and materializes its predetermined prepared
     launch atomically; acknowledgement loss replays the same durable result.
   - Let the logical launcher prepare an intent from the exact clean `DETACHED`
     release or force-fence snapshot without reserving or consuming an image,
     then validate and run the activation-materialized prepared attempt exactly
     once after the atomic handoff. Replay performs no second reservation or
     physical launch.
   - Compose four independently cursor-bounded lanes for retained generation,
     attachment activation, prepared or active launch attempt, and current
     launch inventory. The service is sequential and no-relaunch: it never
     republishes, reserves or consumes an image, invokes a launcher, or
     reconstructs an opaque writer capability.
8. **Production restore adapter enablement (complete)**
   - The durable five-lane recovery cursor prerequisite is complete: one
     PostgreSQL row per recovery scope and lane persists keyset position,
     cycle, revision, and exact transition replay evidence. A bounded runner
     processes the lanes in order and commits each settled continuation before
     admitting the next lane. The fixed-delay production scheduler now invokes
     that runner under the database-global exclusive lifecycle lease.
   - The canonical detach prerequisite is complete: one provider-neutral
     facade holds the per-operation advisory guard across typed release or
     force-fence admission, provider execution, proof validation, and durable
     finalization. Retained ambiguous dispatch cannot replay a storage call,
     while a valid proof can replay only its database finalizer after
     acknowledgement loss. The facade is caller-driven and intentionally not
     scheduled as an autonomous saga.
   - The full publication-binding transport prerequisite is complete: stopped
     backend contract version 3 carries the complete authority-issued
     generation binding to fresh publication or committed-only verification,
     while the legacy callback retains its historical reduced binding.
   - Capture-bound activation-to-launch execution is complete.
   - The durable stop-to-prepared-capture handoff is complete. Migration 006
     adds the permanent `writer-stop-capture-intent-v3` operation-ID claim and
     database triggers that require the claim before a V3 stop can progress
     and require its materialized stop relation before the capture operation
     can exist. A V3 stop request embeds the exact capture intent; its dispatch
     transaction preclaims the capture operation ID before physical stop, and
     one `SERIALIZABLE` finalizer commits the stop while materializing the
     exact capture operation, reservation, and session active pointer as
     `prepared`.
   - Fresh V3 creation is default-closed by
     `writerLaunchStopV3FleetCompatible`, while exact replay and recovery of
     existing V3 work remain available. Legacy V1/V2 stop behavior and the
     same-process stopped-writer capability path are unchanged.
   - Cold capture recovery is complete for the durable handoff. Only an exact
     `prepared` candidate may obtain the single `prepared -> starting`
     dispatch grant and call fresh-only
     `resumePreparedCheckpointCapture()`. A `starting` or `uncertain`
     candidate is source-free and committed-only, and ambiguity after a grant
     never permits a second publication. The V3 local writer exclusion is
     retired only after the exact predetermined committed result returns.
   - The cross-process restore lifecycle guard and recovery scheduler are
     complete. One database-global versioned advisory-lock identity gives
     foreground work a shared lease and each bounded five-lane pass an
     exclusive lease. The runner and service revalidate that lease at their
     lane, candidate, and cursor boundaries. Startup runs one immediate pass;
     later fixed-delay ticks do not overlap, and shutdown drains admitted
     work before releasing the lease.
   - Foreground composition phase A is complete. A caller-persisted contract
     version 1 root plan binds the outer restore request, source checkpoint
     artefact, destination plan, stable `captureCreatedAt`, detach mode,
     holder, image plan, and lease duration. Domain-separated digests derive
     stable renewal, capture, generation, detach, activation, and launch IDs;
     the existing launcher and capture authority still produce the formal
     stop-operation and capture-attempt identities. The fresh safety-capture
     path is resolved by the capture backend and is not the plan's source-
     artefact path.
   - The phase-A facade re-evaluates whether each invocation is fresh or an
     exact typed durable continuation. It requires default-deny detached-
     production compatibility only for fresh work, admits an already-
     materialized V3 stop-to-capture handoff or later typed work through
     authority readback, and holds one shared lifecycle lease across renewal-
     before-stop, V3 stop/prepared capture, generation V1 publication, release
     or force-fence detach without fallback, activation V2, and prepared
     launch. A stop that may have started before the atomic handoff remains
     blocked. The factory requires the nested per-operation guard pool to be
     distinct from both lifecycle pools before any connection is acquired.
   - The standalone phase-A facade is caller-driven retry, not an autonomous
     durable saga. Its stable-plan resolver must return the exact plan.
     Retained or ambiguous subordinate state is interpreted only by the
     existing typed authorities; the facade does not infer completion or
     repeat a physical side effect.
   - The production-neutral phase-B assembly is complete. One strict factory
     constructs a private capture backend, private foreground composition,
     immutable public backend, idle scheduler, and narrow writer-launch plus
     image-plan-reservation ingress from a single internal authority graph, one
     authority/store pool, and three pairwise-distinct nested/foreground/
     recovery guard pools. The launch ingress exposes only `runLaunch()` and
     `reconcileLaunchAttempt()` from the same process-local launcher used by
     capture and foreground restore. Construction performs no migration,
     scheduler lifecycle action, provider action, or pool close. Its public
     backend remains a low-level uncontrolled capability until claimed by the
     controller.
   - The assembled runtime now treats local supervisor state as an explicit
     routed resource. Supervisor and collector exact surfaces carry the same
     persistent high-entropy `stateOwnerId` from the private state-root marker;
     matching strings are necessary but not sufficient in production.
     Deployment requires the exact process-local pair returned by
     `createPodmanWriterSupervisorBundle()` before constructing the physical
     adapter. First owner preparation writes and syncs a complete canonical
     marker in a unique same-parent staging directory, atomically renames that
     directory to the final root, then revalidates and repeats the file, root,
     and parent barriers. A pre-rename crash leaves only inert staging debris;
     a post-rename or lost-ack retry adopts only the complete exact marker.
     Existing malformed or unmarked final roots remain fail-closed. Owner
     preparation and state/supervisor bundle construction fail closed before
     physical dispatch; direct
     `createPodmanWriterSupervisor()` carries only a caller-asserted owner and
     cannot satisfy that deployment boundary.
     Private list wrappers inject that owner into the third launch-attempt and
     fifth supervisor-state-GC authority queries, so neither external lane can
     select a foreign root. Runtime derives the actual cursor scope with
     domain-separated SHA-256 over the caller's base `recoveryScopeId` and the
     owner marker: equal base labels remain isolated across roots, while a
     same-root restart is stable. The base label is not reused as owner.
     The first four lanes retain scalar session cursors. The fifth lane stores
     the full `(sessionId, authorizedAt, terminalOperationId)` boundary and may
     process several items from one session in one bounded page. A pending item
     advances only the current cycle, so later same-session work is not hidden
     and the item is retried after wrap.
     Runtime fixes the same owner into its private foreground composition, whose
     launch-attempt read uses exact `{ operationId, stateOwnerId }`; public
     restore admission has no owner selector.
   - The PostgreSQL durable stable-plan registry slice is complete. Migration
     7 adds immutable canonical admission and plan storage plus a permanent
     operation-ID claim. Separately gated provisioning performs insert or
     exact replay and accepts a lost commit acknowledgement only after exact
     durable readback; crossed identity fails closed. Resolution verifies the
     expected canonical session and remains read-only, so foreground lookup
     cannot write a new plan before its fresh-work fleet gate. The subsequent
     generation dispatch must present the complete rehydrated plan and match
     its digest, generation ID, and destination-isolation proof ID to the
     permanent preclaim before publication authority can be granted.
   - The runtime constructs that registry with its existing internal store,
     exposes only a frozen null-prototype `stablePlanProvisioning` facet, and
     passes a private receiver-preserving resolver to foreground execution.
     Its low-level surface also exposes a narrow same-store `bootstrap.migrate`
     capability for the deployment lifecycle owner.
   - The deployment controller now performs migration before serving, starts
     the scheduler, requires its immediate coalesced pass to prove a complete
     five-lane sweep, and only then opens the gated checkpoint backend, image-plan-
     reservation, stable-plan, and writer-launch facets. Stop closes
     admission, stops the scheduler, and drains all accepted calls without
     closing the four borrowed pools.
   - The PostgreSQL deployment boundary now accepts exact explicit connection,
     verified-TLS, timeout, application-name, and per-role capacity
     configuration and constructs the four private pools. Before controller
     startup it simultaneously checks out one connection from each pool and
     proves PostgreSQL 13-or-newer, writable-primary state, database identity,
     distinct backend sessions, and one shared advisory-lock domain. This is
     point-in-time startup evidence, not continuous topology monitoring. Stop
     drains the controller before it attempts and awaits closure of all four
     pools; none of those internals is exposed.
   - The deployment-owned image-plan binding is complete. One exact provider
     configuration maps an authentic plan's `imagePlanId` to exact OCI
     manifest/config bytes and trusted Codex inspection, then returns an opaque
     process-local reservation through a gated preparation-only facet.
     Resolver and inspector Promises must settle exact frozen null-prototype
     records so inherited `then` cannot replace evidence before validation.
     Foreground and the logical launcher use the same private binding for later
     revalidation.
     This does not fetch images, verify signatures, pin or launch a container
     runtime, implement a supervisor/provider/storage adapter, or create a
     physical writer fence.
   - The physical-collaborator settlement foundation and its first deployment
     consumer are complete. Image-plan resolution and trusted Codex inspection
     each receive their own explicit result deadline and post-deadline
     settlement grace. Provider contract version 2 receives one fresh opaque
     invocation identity and authentic abort signal per call. A deadline closes
     result acceptance and asks the collaborator to abort; a settlement during
     grace is still an uncertain operation, not late success. Failure to settle
     through grace invokes only the deployment-owned private fatal hook, closes
     admission, and cannot by itself prove callback, process, network, or
     physical-effect quiescence or authorize another dispatch.
   - Restore activation now closes its duplicate-dispatch gap before the rest
     of the physical graph receives settlement. The coordinator performs the
     PostgreSQL claim, read-only reconciliation, optional first attach, and
     finalization inside one per-operation guard, and never accepts a serialized
     grant from foreground or recovery callers. Exact `applied` evidence
     finalizes, `absent-and-quiescent` permits the first attach only from that
     same guarded claim, and `unknown` or retained work stays blocked. Durable
     activation request/result version 1 and the schema are unchanged.
   - The complete currently assembled physical graph now has deployment-owned
     method-specific settlement: supervisor launch/reconcile/returned stop,
     `supervisorStateCollector.collectTerminalState`, nine storage-lifecycle
     calls, four publication calls, and restore-destination resolution, in
     addition to the two image-provider calls.
     Transient invocation context never enters durable records. Deployment
     stops all twenty boundaries before pool closure, and any failed drain is
     a sticky failed deployment rather than proof of physical quiescence.
     The destructive supervisor-state collector additionally retains its
     invocation and aggregate stop after a fatal grace breach until the raw
     native Promise settles. Its fifth recovery lane therefore keeps the
     database-global exclusive lifecycle lease during normal operation.
     Connection or database loss may release that advisory lease without
     proving callback quiescence; a same-authorization cold overlap then relies
     on exact concurrent idempotent-or-fail-closed collection.
     The raw Podman/physical supervisor, physical facade, logical supervisor,
     logical reconcile receipt, collection surface/receipt, and aggregate
     binding are versions 5, 4, 4, 2, 2, and 4. The raw launch receipt remains
     version 2, while the raw reconciliation receipt is now version 2.
     Migration 009 binds each
     launch attempt immutably to its local owner before dispatch; GC
     authorization/request/receipt repeat the marker and completion rechecks it.
     Updates to that owner route are forbidden, and its deferred delete guard
     permits removal only with same-transaction teardown of the permanent
     operation-ID claim, preventing delete-and-reinsert ownership transfer.
     The durable launch request remains version 1. Migration 009 refuses any
     legacy `starting`/`uncertain` launch and any non-null session current-launch
     pointer regardless of its shape or referential validity, thereby requiring
     a committed current launch to be stopped or physically fenced before
     rollout and its current-launch pointer to be cleared. It then uses a
     deferred database constraint to keep an already-running old binary from
     committing an ownerless dispatch. Only unbound prepared work remains
     owner-neutral for read/cancel cleanup;
     historical unbound terminal work that is no longer current receives no new
     GC or adoption authority. The marker is routing
     identity, not cryptographic host attestation or protection against an
     administrator cloning the root and marker together.
   - Operational lease admission is now complete. Deployment derives separate
     renewal-to-generation-claim and activation-to-launch-claim bounds from the
     applicable method-specific deadline plus grace periods, an explicit
     aggregate database-request allowance, and a positive safety margin. The
     two independently minted leases use the maximum window rather than a sum;
     stable-plan provisioning and every resolution enforce the same exact
     configured duration before physical work.
   - The completed version 2 assembled safety-matrix slice classifies the twenty
     physical contracts before claiming coverage. Fifteen belong to the private
     protocol surface: nine mutators and six repeatable read-only resolver,
     verifier, inspector, or observer-only reconciliation leaves. The
     supervisor-state collector retains cut
     `supervisor-state-gc`, durable key
     `authorization.terminalOperationId`, and independent overlay
     `supervisor-state-mutator`. The complete
     `supervisor.reconcileWriterLaunch` leaf is conservatively the ninth mutator
     in `supervisor-mutator`, with cut `writer-launch-retirement` and durable key
     `attempt.launchAttemptId`; the matrix now has nine durable cuts and six
     overlays. The other five generic lifecycle methods remain contract-only in
     this saga. Its evidence combines nine real-PostgreSQL durable-cut/commit-
     acknowledgement-loss paths with a same-database/stable-plan retry through
     fresh physical bindings, image binding, runtime, and controller plus
     separate stable-plan-registry rehydration. Settlement evidence remains
     layered: the foundation
     proves aggregate stop ownership and representative deadline/grace
     semantics, while deployment fake-PostgreSQL scenarios exercise the image
     boundary, late settlement, abort/drain, fatal grace breach, and zero calls
     to the durable families they cannot reach. A test-only callback router
     locks the exact fresh-publication versus committed-verification choice at
     the explicit foreground seam; the final public backend now owns that same
     closed choice without exposing the callback. This does not claim one
     whole-saga deployment restart, operating-system `SIGKILL`, or fake-
     PostgreSQL execution of all five collaborator families.
   - The no-second-dispatch property is scoped by authority. One settlement
     invocation is never automatically retried, and the seven one-shot mutators
     remain at-most-once for their operation grants. Exact revision 4 cold
     retirement may repeat only idempotent removal plus the same name/ID absence
     proofs while the record remains durable. It returns the terminal record and
     uses the owner-bound GC finalizer only after both proofs. Ambiguous removal,
     proof, adaptation, or a pre-commit finalizer failure preserves revision 4
     and commits no database finalization. A post-COMMIT acknowledgement loss
     may instead follow an atomic commit of the operation and owner-bound GC
     authorization; exact authorization readback determines whether that commit
     exists. Revision 4 remains until the authorized collector removes it in
     either case. Observer-only reconciliation remains null-record and no-GC.
     Trusted image observations, destination resolution, and committed
     verifiers may repeat in a separate recovery attempt but cannot mint a grant.
     A fresh image reservation is therefore permitted for the same fixed
     prepared plan and is not mutation replay.
   - The final public restore-capable backend is complete. Runtime assembly
     keeps the capture backend private, constructs a second immutable backend
     after foreground composition, and binds its restore authority without a
     mutable placeholder or method swap.
   - Runtime, controller, and deployment expose only the checkpoint facade
     through ready and in-flight admission. The raw lifecycle methods, the
     two-argument `runRestore()` callback seam, operator reconciliation, and
     provider attachment methods remain private,
     preserving the no-second-writer boundary across acknowledgement loss,
     restart, and ambiguous publication, launch, registration, stop, or
     finalisation outcomes.

The production-injectable Linux ext4 and rootless Podman components are now
complete independently for clean, manually fenced operation. They supply an
FD-bound raw-image lifecycle below host-owned `rprivate` carriers in one
long-lived private mount namespace, externally anchored provider state,
distinct publication-control identities, and two-host clean detach, transfer,
and remount verification plus a producer peer-namespace non-propagation gate.
Their trusted persistent-identity bridge and same-process conformance evidence
are complete. Migrations 010 and 011 now provide the PostgreSQL permanent
operation index and the provider-state version 3 cut. A provider-locked version
2 reducer proves the complete revision sequence, storage lineage, pending set,
final projection, and attachment origins before the filesystem publishes a
covering version 3 candidate. PostgreSQL then imports or verifies every record
and advances the head, completeness marker, and deterministic adoption receipt
in one serializable transaction. A database-supplied transaction ID and
deferred revision-coverage check prevent a copied receipt, partial row set, or
head-only update from opening the legacy checksum exception. Commit
acknowledgement loss is settled by exact head, marker, receipt, and full-row
readback. The original full-array adoption authority remains at contract
version 1; the explicit paged authority is contract version 2. Restartable
operation and storage cursors feed fixed four-item pages through staging and
replay in `pg_temp` relations created with `ON COMMIT DROP` inside the same
`SERIALIZABLE` transaction. Pager versions,
cursors, and page boundaries are excluded from the manifest, preserving the
exact version 1 manifest bytes independently of page partitioning. Migration
011, its import window, and its deferred coverage checks remain unchanged, as
do the candidate, `pending`, acknowledgement-loss, and `verified` recovery
phases.
An internal unique event-revision registry validates migrated indexed markers
and maintains completeness inductively: ordinary writes append exactly one
claimed revision or rotate without changing it, while adoption retains its
full-range deferred proof. Raw head insertion or multi-revision jumps cannot
assert a complete permanent index. Stored heads exclude revision zero and a
database-managed progress transaction ID fences any same-transaction second
head mutation after an early constraint check.

Version 3 checkpoints retain current storage, destroyed tombstones, attachment
origin IDs, and the live prepared recovery working set; committed operation
history remains only in PostgreSQL and is available for arbitrary-age exact
replay. Cold open submits one compact projection to the contract-version-3
runtime authority. In one serializable transaction it reads the exact head,
then streams and fully normalizes the complete prepared set through one data
`SELECT` whose `LIMIT` comes from that head's structural bound. It validates
`A` attachment origins in independent fixed 65,535-ID input/query batches
through `max(1, ceil(A / 65,535))` streamed origin data `SELECT` statements,
in addition to the exact-head `SELECT`; SQL parameters and additional memory
remain bounded per batch. It returns a domain-separated receipt only for an
exact match. That receipt is cached only for the same exact loaded head and
authority instance. Native writes continue to store
`indexed-frame-v1`; the one adoption transaction alone may store
`unavailable-adopted-v2`. The full-array adoption version 1 contract remains
operationally capped at 65,535 operations, 65,535 storages, and 64 MiB of
aggregate canonical operation/prepared-projection/storage material, and fails
with `state_capacity_exhausted` before candidate mutation. Paged contract
version 2 removes only those full-array version 1 transport limits. The 4 MiB
record/frame-payload, active-tail 65,535-frame/64 MiB, uint32 checkpoint-count,
and legal version 3 runtime-projection bounds remain distinct and unchanged. See
`linux-ext4-physical-backend.md`. Later slices own
production crash-prefix composition, sudden-power-loss/controller-cache-loss
evidence, automatic stale-writer fencing, differential export/compression,
content-addressed distribution, encryption, registry trust, remote transport,
and broader operational hardening. A separate completed conformance slice
covers only a stopped, non-forking fixture, LVM's filesystem-freeze/flush
snapshot boundary, raw-artifact byte stability, and tail repair on an
independent writable copy; it changes no production API or capability.

Later pull requests may be split further when an experiment reveals a narrower
stable boundary. They must not be combined in a way that hides an experimental
result inside production implementation.

## Parallel Work Within One Pull Request

The repository keeps pull requests serial, but one pull request can use parallel
read-only or isolated tasks for:

- upstream Codex source analysis;
- test-matrix and failure-mode design;
- security, evidence-redaction, and compatibility review.

The integrating agent owns shared files, live-auth scheduling, final code,
delivery gates, and merge orchestration.

## Deferred Work

- **Git Summary** is intentionally deferred. It may later report read-only user
  context such as branch, commit, cleanliness, and insertion/deletion counts,
  but it is not part of snapshot correctness or recovery.
