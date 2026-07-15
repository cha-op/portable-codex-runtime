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
- External-auth probe workstream:
  `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`

## Global Blockers

- The app-server `chatgptAuthTokens` integration is experimental and requires a
  pinned Codex binary or image plus compatibility testing on upgrades.
- Codex rollout flush is not a stable-storage sync barrier. The implemented
  repair covers only pinned plain-JSONL tail framing on a detached restored
  copy; production recovery still needs external sync/freeze, atomic crash
  capture, trusted OCI resolution, fencing, and launcher admission.
