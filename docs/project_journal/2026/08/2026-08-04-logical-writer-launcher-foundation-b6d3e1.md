---
id: 20260804-b6d3e1
title: Logical Writer Launcher Foundation
status: completed
created: 2026-08-04
updated: 2026-08-04
branch: wip/logical-launcher-composition
pr: https://github.com/cha-op/portable-codex-runtime/pull/25
supersedes: []
superseded_by:
---

# Logical Writer Launcher Foundation

## Summary

Added the first half of logical writer-launcher composition: a hardened
PostgreSQL facade that consumes one original image reservation only after the
durable attempt reaches `starting`, invokes an external launcher once, and
registers a provisional same-process writer before finalising the started
attempt. Active-attempt recovery never relaunches. Typed launch-stop authority
and bounded prepared/active/current-launch discovery are also available, while
production restore remains fail-closed.

## Current State

- `createPostgresLogicalWriterLauncher()` returns the exact frozen
  `runLaunch`, `reconcileLaunchAttempt`, and `resolveStoppedWriter` facade.
- A new launch revalidates the original one-use image reservation before
  reserving its durable attempt. It must durably claim `starting` before
  consuming that same reservation, compare the consumed measurement with the
  durable request, and invoke the external launch callback at most once.
- A started result is registered with the designated
  `StoppedWriterCapabilityCoordinator` before the authority finalises it. The
  writer remains provisional until exact finalisation and readback succeed;
  only then may `resolveStoppedWriter` expose the exact same-process handle.
- Prepared recovery cancels without consuming an image or launching. For
  `starting` or `uncertain`, an exact same-process provisional record first
  retries started finalisation with its original evidence; otherwise recovery
  asks the trusted stopped-only supervisor path to reconcile. Neither path
  calls the launcher. An already-started attempt can be adopted only from its
  exact local record; a missing handle requires stop or physical fencing.
- `writer-launch-stop-v1` keeps the original successful launch attempt as the
  immutable source of the current-launch relation. Only exact
  `complete-stopped` evidence from the bound supervisor can clear that
  relation; incomplete or uncertain stop state remains a blocker.
- Bounded keyset discovery exposes prepared or active launch attempts and
  relationally validated current launches without treating either result as
  authority to dispatch or reconstruct a process-local capability.
- The launcher resolver is a same-process bridge for later capture
  composition. It binds the launch's Codex session, root thread, image digest,
  and the first complete normalized capture tuple. This slice does not yet
  route coordinator stop through the durable launch-stop transition or enable
  production capture/restore wiring.
- Exact supervisor stop confirmation removes the facade's strong attempt and
  attachment indexes so completed writers do not accumulate with launch
  history. A rejected or uncertain stop retains its local record because the
  writer may still be running.
- The facade rejects committed authority outcomes unless cancellation uses
  revision 1 and launch terminal outcomes use revision 2 or 3. A newly
  finalised receipt must carry the complete terminal `lastOperation` anchor,
  including its result digest. Historical readback may coexist with a later
  active operation or retain a later committed anchor only while every current
  launch digest and its durable start time still bind the original attempt.
- Historical stop-finalization and stop-claim replays return a top-level
  launch only when the current pointer still belongs to the original stopped
  attempt. A later writer remains visible in the returned session snapshot
  without being misattributed to the older stop receipt.

## Safety Decisions

- Image measurement, generation, process, writer, supervisor, and proof IDs
  remain correlation data. They cannot replace the original opaque image
  reservation, the provisional writer handle, or exact supervisor evidence.
- A reservation-consumption or callback ambiguity after durable `starting`
  leaves the launch attempt blocked for reconciliation; it never permits a
  second launch.
- A session-read, image-revalidation, or guard-admission failure before the
  first durable reservation call returns a retryable admission error. It does
  not claim that this invocation created a durable launch outcome. Retry keeps
  the same launch-attempt ID so a prior replay or concurrent holder is resolved
  by the next reservation/readback path; a failed image inspection additionally
  requires a fresh opaque reservation.
- Register-before-finalise prevents a database acknowledgement loss from
  turning a possibly running writer into an untracked same-process writer.
  Recovery does not synthesize or deserialize a replacement handle.
- A stop operation does not rewrite or retire the original started attempt.
  PID disappearance, exit status, lease expiry, storage detach, and copied
  status fields do not clear the current launch.
- Local record reclamation requires exact stop confirmation. A callback error
  or non-confirming result remains `lost` and retained for explicit stop or
  fencing recovery rather than being reclaimed speculatively.
- Durable replay receipts separate current session state from operation-local
  correlation: a successor launch never becomes evidence for an earlier stop.
- The production checkpoint adapter remains capture-only and `runRestore()`
  remains fail-closed.
- A historical started attempt with `launch: null` after a separate durable
  stop remains fail-closed until the next slice defines an explicit joined
  stop receipt; serialized history never reconstructs the local handle.

## Next Steps

1. In the next serial pull request, bind typed generation claim and exact
   publication through launcher dispatch and no-relaunch recovery, then wire
   the durable stop callback and bounded recovery service.
2. Enable `runRestore()` only after that complete protocol remains fail-closed
   across ambiguous publication, launch, registration, stop, and finalisation
   outcomes.
3. Implement the later filesystem-image backend, differential export,
   retention, and cross-host recovery verification.

## Non-Goals

- No production `runRestore()` enablement.
- No complete production stop/capture composition.
- No concrete Podman or Docker launcher.
- No cross-process reconstruction of opaque reservations or writer handles.
- No filesystem-image backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-logical-writer-launcher.mjs`
- `src/postgres-session-authority.mjs`
- `test/postgres-logical-writer-launcher.test.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `integration/postgres-session-authority.mjs`
- `createPostgresLogicalWriterLauncher()`
- `createWriterLaunchStopOperationRequest()`
- `PostgresSessionAuthority.claimWriterLaunchStopDispatch()`
- `PostgresSessionAuthority.finalizeWriterLaunchStopped()`
- `PostgresSessionAuthority.listWriterLaunchAttemptRecoveryCandidates()`
- `PostgresSessionAuthority.listCurrentWriterLaunchRecoveryCandidates()`
- `README.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
