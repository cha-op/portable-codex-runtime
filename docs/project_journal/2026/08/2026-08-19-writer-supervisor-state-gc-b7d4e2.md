---
id: 20260819-b7d4e2
title: Terminal Writer-Supervisor State GC
status: completed
created: 2026-08-19
updated: 2026-08-20
branch: wip/podman-supervisor-state-gc
pr:
supersedes: []
superseded_by:
---

# Terminal Writer-Supervisor State GC

## Summary

- Completed authority-owned bounded collection of terminal local Podman
  writer-supervisor state after exact PostgreSQL terminal commit and callback
  quiescence.
- Added migration 009's permanent authorization/completion ledger and the
  immutable state-owner route plus the fifth recovery lane, including
  owner-filtered cold-start paging, without granting mutation authority to the
  stopped-only reconciler.

## Current State

- Only owner launch/stop finalizers that commit exact `complete-stopped`
  evidence may insert or replay a supervisor-state GC authorization. Migration
  009 first binds each launch attempt immutably to the private root's persistent
  high-entropy `stateOwnerId` (`state-owner:<64 lowercase hex>`), then binds GC
  authorization to that owner, the terminal operation, and launch attempt. It
  retains the exact stopped revision 4 `terminalRecord` and digests and
  permanently records `collected` or `absent` completion. The stopped-only
  launch reconciler remains a pure read and ordinary callers cannot choose a
  foreign owner.
- In the assembled production runner, the fifth `supervisor-state-gc` recovery
  lane runs last while that runner holds the database-global exclusive restore
  lifecycle lease. Private wrappers inject the local owner into the third and
  fifth authority lists. The fifth lane invokes the independently settled
  `supervisorStateCollector.collectTerminalState` physical leaf with exact
  `{ stateOwnerId, terminalRecord }`, validates the version-2 receipt repeats
  that owner, and rechecks it before durable completion. Cold-start recovery
  and exact ledger readback preserve progress across process restart.
- Runtime derives its effective `recovery-owner:<64 hex>` cursor scope with
  domain-separated SHA-256 over the configured base `recoveryScopeId` and
  local owner marker. Reusing one base label across two roots no longer lets
  either third/fifth lane skip the other's rows; same-root restart derives the
  same scope. The base label is not owner identity.
- Production deployment accepts only the exact process-local supervisor and
  collector pair returned by `createPodmanWriterSupervisorBundle()` and checks
  that provenance before constructing the physical adapter. Matching
  `supervisorId` and `stateOwnerId` strings are necessary but insufficient.
  Direct `createPodmanWriterSupervisor()` carries a caller-asserted routing
  owner only and cannot enter production deployment. Owner preparation and
  state/supervisor bundle construction fail closed before physical dispatch.
- Runtime fixes the validated owner into its private foreground composition.
  `readWriterLaunchAttempt()` receives exact
  `{ operationId, stateOwnerId }`; the public checkpoint facade and restore
  admission expose no owner selector.
- Local collection is two-phase. Phase 1 validates an intact revision 0 through
  4 chain and all present sidecars, or admits only the oldest-first missing
  lower retry prefix while exact revision 4 remains. It removes revisions 0
  through 3 plus sidecars, preserves revision 4 as the terminal anchor, and
  syncs the held directory. Phase 2 compares the named revision 4 object and
  bytes with its held FD, unlinks it, then positionally rereads exact bytes
  through that FD while revalidating identity and access policy. It proves all
  attempt artifacts absent and syncs the directory again. A lost `collected`
  acknowledgement may retry as `absent`; PostgreSQL completion remains exact
  and idempotent.
- The assembled physical graph now has twenty settlement leaves. The collector
  is the eighth protocol mutator, with leaf
  `supervisorStateCollector.collectTerminalState`, cut
  `supervisor-state-gc`, durable key
  `authorization.terminalOperationId`, and independent overlay
  `supervisor-state-mutator`.
- The transient raw Podman supervisor is version 4, its launch receipt remains
  version 2, the logical facade is version 3, the collection surface/receipt is
  version 2, and the aggregate physical binding is version 3. The durable
  logical launch request, evidence projection, and local revision records
  remain version 1, so owner routing does not change existing request hashes.

## Safety Boundary

- Object identity is `dev`/`ino` plus held directory/file descriptors. Content
  stability includes the post-unlink held-FD positional reread of exact
  canonical bytes. Access policy separately checks same-UID regular files at
  `0600` with required `nlink`, the same-UID state root and immediate parent at
  `0700`, and safe ownership/write/sticky policy on traversal ancestors. Child-
  entry or generic `stat` churn triggers revalidation but is not evidence of
  replacement or mutation.
- A same-authorization cold overlap may remove only the prevalidated
  record/pending sibling aliases. Held-FD link count can decrease monotonically
  within its prior bound but cannot increase, and every held artifact must
  reach zero links at the final absence proof.
- Pre-mutation I/O or unreadable state, canonical-chain or terminal conflict, and
  post-mutation outcome uncertainty are distinct fail-closed classes. A proved
  already-absent attempt is a successful replay, not uncertainty.
- The local collector validates and executes one exact deletion but does not
  prove PostgreSQL authority or callback quiescence. The production path gets
  those properties from the owner finalizer, outer database-global lifecycle
  exclusion, and the collector's separate physical settlement.
- The state-root marker prevents accidental cross-root routing but does not
  prove cryptographic host identity and cannot detect an administrator cloning
  both root and marker. Missing, malformed, or mismatched markers fail owner
  preparation or bundle construction before physical dispatch. Active legacy
  attempts without a binding are quarantined and have no adoption API; only
  unbound prepared attempts remain owner-neutral for read/cancel cleanup.
- The destructive collector's deadline-plus-grace breach aborts and reports a
  fatal deployment failure but retains the active invocation and aggregate
  stop until the raw native Promise settles. This keeps the fifth lane's
  exclusive lifecycle lease held during normal operation while old deletion
  work may still run. Connection or database loss may release that advisory
  lease without proving quiescence; a same-authorization cold overlap then
  relies on exact concurrent idempotent-or-fail-closed collection.

## Next Steps

- Define an authority-safe provider-state exact-replay retention floor or move
  permanent provider operation history to a PostgreSQL-indexed representation.
  This independent track must preserve the origin operation for every current
  attachment and does not inherit authority from supervisor-state collection.

## Evidence

- PostgreSQL authority and migration:
  `migrations/authority/009-writer-supervisor-state-gc.sql`,
  `src/postgres-session-authority.mjs`, `src/postgres-serializable-store.mjs`
- Local state and physical binding:
  `src/podman-writer-supervisor-state.mjs`,
  `src/podman-writer-supervisor.mjs`,
  `src/postgres-detached-restore-physical-bindings.mjs`
- Recovery and composition:
  `src/postgres-restore-activation-recovery-service.mjs`,
  `src/postgres-restore-recovery-runner.mjs`,
  `src/postgres-restore-recovery-cursor-store.mjs`,
  `src/postgres-detached-restore-runtime-composition.mjs`,
  `src/postgres-detached-restore-foreground-composition.mjs`,
  `src/postgres-detached-restore-runtime-controller.mjs`,
  `src/postgres-detached-restore-deployment.mjs`
- Focused coverage:
  `test/podman-writer-supervisor-state.test.mjs`,
  `test/postgres-session-authority.test.mjs`,
  `test/postgres-logical-writer-launcher.test.mjs`,
  `test/postgres-restore-activation-recovery-service.test.mjs`,
  `test/postgres-detached-restore-foreground-composition.test.mjs`,
  `test/postgres-detached-restore-runtime.test.mjs`,
  `test/postgres-detached-restore-runtime-controller.test.mjs`,
  `test/postgres-detached-restore-deployment.test.mjs`,
  `test/postgres-detached-restore-assembled-safety-matrix.test.mjs`,
  `integration/postgres-session-authority.mjs`
