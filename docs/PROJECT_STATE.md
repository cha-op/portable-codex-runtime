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
- Bounded checkpoint recovery now enumerates retained `starting` or `uncertain`
  capture operations through
  `PostgresSessionAuthority.listCheckpointCaptureRecoveryCandidates()`. It uses
  immutable `session_id` keyset order, the existing active-operation index, a
  hard `limit + 1` page query, and same-snapshot relational validation without
  schema DDL. The single-backend recovery service processes one page
  sequentially from frozen startup configuration and passes only exact durable
  `{checkpoint, request}` admissions to committed reconciliation. Its
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
  constraints.
- Typed restore-generation authority now reserves the exact
  `{checkpoint, request}` admission, claims one generation and
  destination-isolation proof under the database-clock lease and restore
  fence, and atomically finalises the committed generation with its operation,
  reservation, and session terminal anchor. Claim replay never grants a second
  dispatch; exact finalisation replay and bounded `starting`/`uncertain`
  recovery remain available on the migration version 2 schema.
- Typed durable writer-launch attempts now reuse the permanent operation and
  reservation rows without migration version 3. One operation ID is also the
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
  the original attempt. Exact local stop confirmation
  releases the facade's strong attempt and attachment indexes; an uncertain
  stop retains the record fail-closed instead of treating a possibly running
  writer as reclaimable.
- Typed `writer-launch-stop-v1` authority preserves the original started
  attempt and clears the current-launch relation only for exact
  `complete-stopped` evidence from the bound supervisor. Historical stop or
  claim replays expose a current launch only when it still belongs to that
  stopped attempt, so a successor remains visible in the session snapshot but
  is never attached to the old receipt. Bounded keyset discovery returns
  prepared or active launch attempts and relationally validated current
  launches without reconstructing process-local authority.
- Restore-generation request version 2 durably records the exact measured
  image, supervisor, and launch-attempt identity before publication begins.
  One serializable authority transition commits the generation, retires the
  restore operation, creates the exact prepared launch operation and
  reservation, and advances the canonical session twice. The launcher can
  prepare the process-local image capability before publication and later
  claim only that pre-reserved attempt, so a crash cannot leave a newly
  committed generation without discoverable launch work.
- The production checkpoint adapter remains capture-only. Restore fails closed
  until later serial pull requests verify committed generation publication,
  route coordinator stop through the durable transition, add bounded
  no-relaunch recovery, and wire the complete protocol into `runRestore()`.
  No published path, generation row, serialized measurement, attempt record,
  or discovery result is writer-launch authority by itself.
- Per-workstream implementation state lives under `docs/project_journal/`.

## Recovery Pointers

- Runtime delivery plan:
  `docs/project_journal/2026/07/2026-07-01-runtime-delivery-plan-6f13a8.md`
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
  authority until exact generation publication, launcher and no-relaunch
  recovery, durable stop/capture composition, and recovery-service wiring are
  integrated into `runRestore()` and verified as one fail-closed protocol.
