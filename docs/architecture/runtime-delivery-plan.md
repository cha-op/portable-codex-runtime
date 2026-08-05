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
- Require GitHub Codex review, an independent Codex PR review, and an offline
  frozen-diff review before merge.
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
   - Keep the production adapter capture-only. Restore fails closed until a
     later slice binds one canonical detached destination generation to
     launcher admission.
6. **Bounded checkpoint recovery service (complete)**
   - Enumerate only retained `starting` or `uncertain` capture operations in
     bounded `session_id` keyset pages, reconstruct their exact durable
     admissions, and invoke only committed, source-free reconciliation.
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
   - Gate version 2 creation on confirmed fleet-wide authority and recovery
     compatibility before production can persist the new request shape.
   - Prepare one exact clean-capture tuple, derive the stop operation from the
     complete tuple and current launch attempt, route one physical stop through
     `writer-launch-stop-v1`, and validate the committed operation,
     reservation, session, and complete-stop proof before issuing one opaque
     capability. Retain the local writer identity until capture succeeds;
     reconcile only the exact same-process `prepared` or explicitly granted
     `starting` pre-dispatch state, accept lease-expiration extension only for
     the same stable writer-fence identity, and block same-session successor
     launch in the canonical local coordinator until retirement. Ambiguous stop,
     finalisation, capture, or retirement stays fail-closed.
7. **Detached restore activation and recovery composition**
   - Keep the absent restore-publication destination independent from the
     current active attachment. After exact committed publication, obtain
     provider-backed attachment evidence for that object and atomically
     replace the canonical session attachment while materializing the exact
     prepared launch request.
   - Require the old attachment to be stopped, fenced, and detached before
     activation. Pathname equality is correlation only and never proves
     attachment authority or filesystem object identity.
   - Verify committed restore publication against the detached destination
     and activated attachment before any prepared launch can proceed.
   - Compose bounded generation, prepared-launch, active-attempt, and
     current-launch recovery without relaunching or reconstructing an opaque
     image or writer capability from serialized state.
8. **Production restore adapter enablement**
   - Wire publication, the atomic handoff, prepared launch, no-relaunch
     recovery, durable stop, and capture composition through the production
     checkpoint adapter.
   - Enable `runRestore()` only after the whole protocol preserves the
     no-second-writer boundary across acknowledgement loss, restart, and
     ambiguous publication, launch, registration, stop, or finalisation
     outcomes.

Later pull requests own an ext4 or filesystem-image backend, differential
export and content-addressed storage, cross-host migration, and operational
hardening.

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
