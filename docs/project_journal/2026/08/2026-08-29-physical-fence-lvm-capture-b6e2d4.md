---
id: 20260829-b6e2d4
title: Physical-Fence LVM Capture Finalization
status: completed
created: 2026-08-29
updated: 2026-08-29
branch: wip/atomic-crash-capture-dispatch-reconcile
pr:
supersedes: []
superseded_by:
---

# Physical-Fence LVM Capture Finalization

## Summary

- Joined the durable force-fence V2 prepared capture to the independent
  PostgreSQL atomic-capture catalogue and classic-LVM provider through a new
  private two-method composition.
- Added dedicated session-authority read and finalization transitions. The
  session capture remains `prepared@0` throughout provider dispatch and may
  move directly to `committed@1` only after exact committed catalogue evidence
  and physical artifact verification.
- Released the capture reservation only into `RECOVERY_REQUIRED`, which keeps
  writer admission closed until the later writable-copy repair workstream.

## Current State

- `runPreparedCapture()` is the only fresh entry point. It requires the exact
  session capture in `prepared@0` and an absent provider row, lets the catalogue
  issue its one durable dispatch claim, then rereads authority after that claim
  and before the single `lvcreate` call.
- `reconcileCapture()` never issues fresh provider work. Provider
  `starting`, `uncertain`, or `committed` state, all ambiguous failures, and
  every existing exact attempt use source-free retained-artifact verification.
- The provider catalogue owns physical single dispatch. The session authority
  independently owns writer admission and retains its active prepared blocker
  while provider state changes.
- `finalizeAtomicCrashCapture()` is default-closed behind
  `atomicCrashCaptureV1FleetCompatible`. It locks and validates the exact
  force-fence, preclaim, capture, reservation, provider request, four catalogue
  identities, and result digest before one serializable terminal transition.
- Finalizer acknowledgement loss uses exact authority readback. A committed
  capture is physically reverified and never redispatched.
- Migration 14 permits only `prepared@0 -> committed@1` for the session
  capture, requires the independently committed provider row, and extends the
  deferred reverse invariant to provider-row changes.

## Protected Properties

- Object identity remains the retained snapshot LV UUID under
  `lvm-lv-uuid-v1`; provider name, tag, origin UUID, and device observations
  cannot substitute for that persistent identity.
- Content stability remains the exact visible byte length and full streaming
  SHA-256. Timestamp or unrelated LVM metadata changes are not treated as
  content mutation.
- Access policy requires both read-only LVM state and a read-only block device.
  Stable identity and bytes do not compensate for writable evidence.
- Writer exclusion is an authority policy property. A committed artifact alone
  cannot reopen writer admission; the terminal session has no lease,
  attachment, launch, or active operation and is explicitly
  `RECOVERY_REQUIRED`.

## Safety Boundary

- The new composition consumes only a prepared capture created by the exact
  authenticated force-fence V2 handoff. It does not create physical-fence
  evidence or widen the ext4 backend's manual-fencing capability.
- The private classic-LVM provider and PostgreSQL catalogue remain dormant in
  public deployment. No ordinary clean-capture or lifecycle facade discovers
  this path.
- This slice does not repair a crash prefix, create a separate writable
  generation, admit a higher-epoch writer, or provide host, controller, or
  drive cache-loss evidence.

## Follow-up

- Repair the crash prefix only on a separately authorized writable generation.
- Replace `RECOVERY_REQUIRED` with an exact repair terminal and admit a writer
  only for that generation under a strictly higher epoch and new lease.
- Keep public recovery selection, cache-loss evidence, retention, export,
  distribution, encryption, and remote provider work separately scoped.

## Validation

- The new composition plus adjacent crash-capture core and LVM provider suites
  passed 95/95 tests.
- The complete session operation-kernel suite passed 324/324 tests, including
  exact provider-state reads, finalization, acknowledgement-loss replay,
  crossed identities and digests, terminal force-fence replay, and pre-database
  successor rejection.
- The PostgreSQL serializable-store suite passed 152/152 tests and the detached
  runtime-controller suite passed 24/24 tests.
- The unfiltered full suite reached only the unchanged live-auth watcher
  failure after the host returned `EMFILE: too many open files, watch`. The
  complete suite passed when only
  `chatgptAuthTokens refreshes after 401 without writing auth.json` was skipped
  by exact name, matching the repository's established local fallback.
- `npm run test:postgres` reached its required environment gate but could not
  run the real database scenario because `SESSION_AUTHORITY_DATABASE_URL` is
  not configured locally. Current-head PostgreSQL CI remains required evidence
  for migration 14 and its deferred provider/session invariants.
- Changed JavaScript modules passed `node --check`, `git diff --check` passed,
  and project-journal validation passed.
- Fresh-context local review, current-head CI, and GitHub Codex evidence are
  recorded in the pull request before merge.

## Evidence

- Session authority: `src/postgres-session-authority.mjs`
- Provider composition:
  `src/postgres-writer-force-fence-lvm-atomic-crash-capture-composition.mjs`
- Migration: `migrations/authority/014-atomic-crash-capture-finalization.sql`
- Unit coverage: `test/postgres-session-operation-kernel.test.mjs`
- Composition coverage:
  `test/postgres-writer-force-fence-lvm-atomic-crash-capture-composition.test.mjs`
- PostgreSQL integration: `integration/postgres-session-authority.mjs`
- Architecture: `docs/architecture/atomic-crash-capture-extension.md`
