---
id: 20260806-c4a2d8
title: Capture-Bound Restore Activation
status: completed
created: 2026-08-06
updated: 2026-08-06
branch: wip/capture-bound-restore-activation
pr:
supersedes: []
superseded_by:
---

# Capture-Bound Restore Activation

## Summary

The production restore adapter previously could not compose the existing
detached activation protocol with durable stop-to-capture. This workstream adds
a new fleet-gated activation request version that proves the actual production
predecessor chain while preserving the existing version 1 request for exact
read, replay, and recovery. Production `runRestore()` remains fail-closed.

## Current State

- `createRestoreAttachmentActivationOperationRequestV2()` records the exact
  old attachment, writer-stop operation, clean-capture operation, and later
  release or force-fence operation.
- `PostgresSessionAuthority` validates the complete committed relation before
  fresh version 2 reservation and again during claim, read, finalization, and
  recovery. The stopped launch generation may differ from the target restore
  generation.
- Fresh restore-generation-v2 and activation-v2 reservation have independent
  default-deny startup backstops. Exact existing operation replay precedes
  both checks.
- The recovery coordinator reconstructs either activation request version
  without changing service candidates, cursor shapes, destination
  verification, provider activation, or atomic activation-to-launch
  finalization.
- Production `runRestore()` remains unavailable pending the separate adapter
  and durable recovery-cursor workstream.

## Discovered Protocol Gap

The version 1 activation request requires a committed writer stop to be the
direct predecessor of release or force-fence and requires that stopped
writer's launch generation to equal the target restore generation. The
production chain instead commits a clean checkpoint capture between stop and
detach, and the current writer normally belongs to a different generation
from the checkpoint being restored.

The existing integration success path therefore proves only this unsuitable
sequence:

```text
target generation -> launch on old attachment -> stop -> detach -> activate
```

The production-safe sequence must be:

```text
target generation publication
-> stop current writer
-> clean checkpoint capture of the old attachment
-> release or force-fence the old attachment
-> activate the detached target destination
-> atomically materialize one prepared launch
```

## Protected Property

No restored writer can acquire writable authority unless one durable relation
proves both sides of the handoff:

- the old attachment's actual writer reached committed complete-stop;
- one committed clean capture binds that exact stop operation, attachment,
  capture attempt, checkpoint result, and catalogue;
- release or force-fence committed after that capture against the same old
  attachment, leaving the canonical session `DETACHED`;
- the target committed restore generation binds that old attachment as its
  authority predecessor but need not equal the stopped writer's generation;
- the independently published destination still passes object-identity,
  content-stability, and access-policy revalidation before provider attach;
- activation still atomically installs the provider-backed attachment and the
  predetermined prepared launch.

Path equality, timestamps, ordinary `stat` changes, a database row, or a
generation identifier alone do not prove this property. Unreadable or failed
revalidation remains distinct from a proved missing or mismatched object.

## Chosen Compatibility Contract

- Keep operation kind `restore-attachment-activation-v1` and add activation
  request `contractVersion: 2`. This follows the existing
  `restore-destination-generation-v1` convention, which already supports
  multiple request contract versions under one durable operation kind.
- Keep request version 1 parsing and relational validation unchanged.
- Add an explicit version 2 creator whose predecessor contains
  `attachmentId`, `stopOperationId`, `captureOperationId`, and
  `detachOperationId`.
- Version 2 relation validation follows durable last-operation pointers and
  complete operation/reservation relations. It does not infer ordering from
  timestamps and does not require the stopped launch generation to equal the
  target restore generation.
- Reuse the existing activation launch-intent registry claim and SQL schema.
  No migration is needed because the operation kind, registry claim shape,
  and stored JSON envelope remain unchanged.
- Teach recovery validation to reconstruct either request version from the
  durable candidate. The four-lane service candidate and cursor shapes remain
  unchanged.

## Fleet Gates

`PostgresSessionAuthority` gains two independent default-deny startup
decisions:

- restore-generation request version 2 compatibility;
- capture-bound restore-activation request version 2 compatibility.

Each backstop is evaluated only after exact durable lookup proves the
operation absent and before any insert or update. Exact replay and recovery of
existing work remain available when a gate later closes. The deployment
control plane must prove all authority, API, and recovery workers sharing the
database understand the selected request version; schema migration state
alone is not fleet membership proof.

## Scope

- Versioned activation request and relational validation in PostgreSQL
  authority.
- Independent generation-v2 and activation-v2 fresh-creation backstops.
- Recovery coordinator compatibility for both activation request versions.
- Unit, operation-kernel, and real PostgreSQL coverage for fresh denial,
  replay bypass, exact stop/capture/detach binding, different source and target
  generations, and version 1 compatibility.
- Architecture and project-journal updates.

## Non-Goals

- No production `runRestore()` enablement.
- No restore callback contract or publication mode change in
  `StoppedDirectoryBackend`.
- No provider attach, prepared-launch dispatch, or recovery cursor scheduler
  orchestration beyond the existing independent components.
- No filesystem-image backend, differential compression, cross-host restore,
  periodic snapshot scheduler, or Git Summary.

## Acceptance Criteria

- Fresh generation-v2 and activation-v2 reservation fails with zero writes by
  default; explicit compatible startup decisions permit only their matching
  protocol.
- An exact existing operation replays while its corresponding gate is closed.
- Version 1 activation retains its direct stop-to-detach and same-generation
  historical semantics.
- Version 2 rejects a missing, non-terminal, substituted, or reordered stop,
  capture, or detach operation and any attachment or capture-binding drift.
- Version 2 accepts a committed clean capture between stop and detach when the
  stopped launch generation differs from the target restore generation.
- Recovery reconstructs the exact version 2 request without weakening
  committed destination or provider proof checks.
- The complete local test suite passes. Real PostgreSQL execution remains a
  required PR CI admission gate because no local
  `SESSION_AUTHORITY_DATABASE_URL` is available.

## Next Steps

1. In a later PR, add the invocation-time detached-production fleet capability
   and compose publication, stop/capture, detach, activation, prepared launch,
   and bounded cursor persistence through `runRestore()`.

## Evidence

- `src/postgres-session-authority.mjs`
- `src/postgres-durable-stop-capture-composition.mjs`
- `src/postgres-restore-activation-recovery-coordinator.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/project_journal/2026/08/2026-08-05-detached-restore-activation-3f91c2.md`
- Historical reverted composition: commit `65559ea`; correction: commit
  `06be793`.
- Local validation used installed Node `v24.18.0`: all changed JavaScript files
  passed `node --check`; authority, operation-kernel, and recovery-coordinator
  focused suites passed; the complete unskipped `npm test` suite passed outside
  the managed sandbox after the sandbox-only macOS watcher `EMFILE` was
  isolated with an exact failing-test rerun.
- Operation-kernel coverage includes release and force-fence success paths,
  non-terminal and binding-drift predecessor rejection before writes, and
  existing activation-v2 recovery while the fresh-creation fleet gate is
  closed.
- `git diff --check` and the project-journal validator passed.
- Real PostgreSQL integration is defined in
  `integration/postgres-session-authority.mjs`; local execution requires
  `SESSION_AUTHORITY_DATABASE_URL` and therefore remains a required PR CI
  gate. The implementation slice is complete; merge admission still requires
  that CI result.
