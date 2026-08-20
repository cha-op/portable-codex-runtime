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
  owner-filtered cold-start paging and the exact durable revision 4 retirement
  path without granting authority to observer-only reconciliation.

## Current State

- Only owner-bound finalizers that commit exact `complete-stopped` evidence
  with a stopped revision 4 terminal record may insert or replay a
  supervisor-state GC authorization. Immediate launch/stop and exact durable
  revision 4 cold retirement use that path. Migration
  009 first binds each launch attempt immutably to the private root's persistent
  high-entropy `stateOwnerId` (`state-owner:<64 lowercase hex>`), then binds GC
  authorization to that owner, the terminal operation, and launch attempt. It
  retains the exact stopped revision 4 `terminalRecord` and digests and
  permanently records `collected` or `absent` completion. Observer-only launch
  reconciliation returns a null terminal record, remains no-GC, and ordinary
  callers cannot choose a foreign owner.
- In the assembled production runner, the fifth `supervisor-state-gc` recovery
  lane runs last while that runner holds the database-global exclusive restore
  lifecycle lease. Private wrappers inject the local owner into the third and
  fifth authority lists. The fifth lane invokes the independently settled
  `supervisorStateCollector.collectTerminalState` physical leaf with exact
  `{ stateOwnerId, terminalRecord }`, validates the version-2 receipt repeats
  that owner, and rechecks it before durable completion. Cold-start recovery
  and exact ledger readback preserve progress across process restart.
- The fifth lane uses the complete
  `(sessionId, authorizedAt, terminalOperationId)` key for authority pages,
  service receipts, runner digests, and durable cursor compare-and-swap. It may
  process multiple items from one session in a bounded page. A permanent
  `pending` item no longer hides later same-session work; the current cycle
  advances past it and a null-boundary wrap retries it. The other four lanes
  retain their scalar session cursor and version-1 digest bytes.
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
- Exact durable revision 4 cold reconciliation first completes idempotent
  `podman rm --ignore`, then proves both anchored-name and exact-ID absence with
  separate `podman ps -a --no-trunc` queries. Only then does its version-2
  receipt carry the terminal record to the owner-bound GC finalizer. Ambiguity
  in removal, either proof, physical adaptation, or a pre-commit finalizer
  failure preserves revision 4 and commits no database finalization. A
  post-COMMIT acknowledgement loss may instead follow an atomic commit of the
  operation and owner-bound GC authorization; exact authorization readback
  determines whether that commit exists. Revision 4 remains until the
  authorized collector removes it in either case. Observer-only
  `complete-stopped` and `not-started` remain null-record and no-GC.
- The assembled version 2 safety matrix still has twenty settlement leaves:
  nine mutators, six observations, five contract-only leaves, nine durable
  cuts, and six overlays. The collector mutator retains leaf
  `supervisorStateCollector.collectTerminalState`, cut
  `supervisor-state-gc`, durable key
  `authorization.terminalOperationId`, and independent overlay
  `supervisor-state-mutator`. The complete reconciliation leaf is
  conservatively in `supervisor-mutator`, with cut
  `writer-launch-retirement` and durable key `attempt.launchAttemptId`.
- The transient raw Podman and physical supervisor is version 5; its launch
  receipt remains version 2, while its reconciliation receipt is now version 2.
  The physical facade and logical supervisor are version 4, the logical
  reconcile receipt is version 2, the collection surface/receipt remains
  version 2, and the aggregate physical binding is version 4. The durable
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
- Linux destructive lookups, unlinks, and absence proofs resolve artifact
  basenames through a revalidated clone of the held state-root FD. A named-root
  replacement therefore cannot redirect deletion to a bait directory. The
  non-Linux path keeps held/named identity and access-policy brackets but does
  not claim protection against an active same-UID ABA replacement because
  Node exposes no portable directory-relative unlink API.
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
  preparation or bundle construction before physical dispatch. Migration 009
  refuses legacy `starting`/`uncertain` launches and any non-null session
  current-launch pointer, including malformed or orphaned pointer data, while
  holding the runtime's session-to-operation lock order. The latter gate
  requires a committed current launch to be stopped or physically fenced before
  rollout and its current-launch pointer to be cleared. Its deferred commit-time
  constraint then prevents already-running old binaries from making ownerless
  dispatch durable. Unbound prepared
  attempts remain owner-neutral for read/cancel cleanup; historical unbound
  terminal work that is no longer current gains no GC/adoption authority and
  remains an explicit legacy cleanup case.
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
