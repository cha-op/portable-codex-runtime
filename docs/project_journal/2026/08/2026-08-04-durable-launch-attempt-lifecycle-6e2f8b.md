---
id: 20260804-6e2f8b
title: Durable Launch Attempt Lifecycle
status: completed
created: 2026-08-04
updated: 2026-08-04
branch: wip/logical-launch-attempt-authority
pr:
supersedes: []
superseded_by:
---

# Durable Launch Attempt Lifecycle

## Summary

Added the typed PostgreSQL writer-launch attempt lifecycle over the existing
operation and reservation schema plus canonical session document version 3.
One attempt binds a committed restore generation to the exact lease,
attachment, fence, measured image, process, writer, and supervisor evidence
without invoking a launcher or enabling production restore.

## Current State

- `operation_claims.operation_id` is also the globally unique launch-attempt
  identity. No launch-specific relation or migration version 3 is required.
- The public request builder accepts the complete committed generation
  snapshot and derives the compact generation reference retained by the exact
  operation request. The request also retains the complete attachment and
  lease snapshot, the fencing epoch, a bounded platform-image projection and
  runtime measurement, and the trusted supervisor identity.
- Generic reservation makes `prepared` durable before dispatch. Typed claim
  locks and revalidates the session, operation, reservation, and committed
  generation, then reads the database clock after those potentially blocking
  locks before granting one definite `prepared -> starting` dispatch. The
  committed generation operation must belong to the same immutable session
  history and satisfy its creation, terminal-revision, and claim-time bounds.
- `starting` and `uncertain` attempts retain both active rows and the canonical
  session pointer. Restart or acknowledgement loss never grants dispatch
  again.
- Exact trusted evidence finalizes an attempt as `started`, `not-started`, or
  `complete-stopped`. Replays require the same canonical evidence.
- Session document version 3 stores the current started-launch pointer
  separately from `lastOperation`. Readback resolves its permanent operation
  and revalidates the operation's immutable session history plus the
  generation, attachment, stable lease tuple, measured image, process and
  writer incarnations, and supervisor proof.
- Canonical version 1 and version 2 documents retain exact read and replay
  compatibility. The next real state write upgrades them to version 3.
- Bounded keyset recovery returns only exact `starting` or `uncertain`
  attempts, including after the current lease later expires.

## Safety Decisions

- A committed generation, published path, journal record, serialized image
  measurement, launch-attempt ID, process ID, writer ID, or proof ID is not
  writer-launch authority.
- The measured-image request is durable comparison evidence only. It does not
  replace or consume the opaque one-use reservation from
  `PlatformImageReservationCoordinator`.
- `complete-stopped` means the trusted supervisor proved that the complete
  container, cgroup, or VM writer boundary joined. PID disappearance, an exit
  code, Codex protocol events, lease expiry, or storage detach is insufficient.
- Writer release requires no current launch. Force-fence dispatch and an
  unresolved blocked result retain the current launch through `starting`,
  `uncertain`, and `BLOCKED`; only an exact successful physical fence clears
  it.
- Lease renewal preserves the current launch and compares the stable lease ID,
  holder, epoch, and attachment while allowing expiration to advance.
- The production checkpoint adapter remains capture-only.

## Remaining Work

1. Compose the committed generation, original one-use image capability,
   durable attempt, external launcher, and exact writer registration.
2. Enable production restore only after the complete composition remains
   fail-closed under dispatch and finalization acknowledgement loss.
3. Implement the filesystem-image backend, differential export, retention, and
   cross-host recovery verification.

## Non-Goals

- No launcher or supervisor callback.
- No Podman or Docker execution.
- No opaque image-capability consumption.
- No production restore enablement.
- No launch-specific SQL table.
- No filesystem-image backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-session-authority.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `integration/postgres-session-authority.mjs`
- `README.md`
- `docs/PROJECT_STATE.md`
- `docs/PROJECT_TODO.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
