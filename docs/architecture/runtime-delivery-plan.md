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

Restore and launcher authority are now split into four serial pull requests:

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
3. **Durable launch-attempt lifecycle**
   - Reserve and claim one exact launch attempt, persist every
     `starting` or `uncertain` blocker, and accept only exact supervisor
     evidence when finalizing a started or stopped attempt.
   - Bind the finalized destination generation, session attachment, lease,
     epoch, image measurement, process incarnation, and writer incarnation
     without invoking a launcher in this slice.
4. **Logical launcher composition and enablement**
   - Atomically compose finalized generation admission, one-use measured-image
     consumption, durable launch-attempt dispatch, the external launcher, and
     exact writer registration.
   - Enable production restore only after the complete composition preserves
     the no-second-writer boundary across acknowledgement loss, restart, and
     ambiguous launcher outcomes.

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
