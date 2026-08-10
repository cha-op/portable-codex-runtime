---
id: 20260810-5d82a1
title: Durable Restore Recovery Cursors
status: completed
created: 2026-08-10
updated: 2026-08-10
branch: wip/restore-recovery-cursors
pr:
supersedes: []
superseded_by:
---

# Durable Restore Recovery Cursors

## Summary

The four-lane PostgreSQL restore recovery service already reconciles retained
generation, attachment-activation, launch-attempt, and current-launch state,
but its returned keyset cursors are process-local. This workstream adds one
durable control-plane cursor per recovery scope and lane plus a bounded
one-step runner that resumes from those cursors after restart. Production
`runRestore()` remains fail-closed.

## Protected Property

The composed runner may advance a lane only after consuming an authentic,
one-use receipt proving that the real recovery service has drained that batch.
Here “settled” means the current reconciliation attempt returned; a business
result may remain `pending` and is revisited after the keyset cursor wraps. The
low-level store separately performs an exact compare-and-swap over observed
revision, cycle, and prior cursor; it is not candidate-settlement authority. A
committed transition binds one transition ID and request digest. A stale
runner, commit acknowledgement loss, abort, or process restart may repeat
existing idempotent recovery work, but must not forge a batch, overwrite a
newer cursor, move another lane, or create restore/publication/launch
authority. Callback-reachable prototype mutation also cannot redirect a lane
or intercept the construction of a private dense result array: control tuples
are read through their own numeric properties, and every private numeric slot
is created with the captured data-property intrinsic before it is trusted.
Runtime validation modes are primitive booleans rather than option bags, so a
callback cannot inject a later-run policy through `Object.prototype`. The
runner also captures its cryptographic constructor before any callback runs,
so synchronizing a mutated Node builtin export cannot change the durable
request digest implementation.

## Scope

- Add a checksum-bound authority migration for four independent restore
  recovery cursor rows per configured recovery scope.
- Implement strict cursor read/initialization, exact transition replay,
  revision/cycle compare-and-swap, null-cursor sweep wrapping, and uncertain
  commit readback.
- Implement a startup-fixed runner that processes generation, activation,
  launch-attempt, and current-launch lanes in order, persisting each settled
  lane before admitting the next.
- Add unit, migration, and real-PostgreSQL integration coverage for restart,
  stale concurrency, abort, partial progress, exact replay, and commit
  acknowledgement loss.
- Update the architecture plan to record this prerequisite without claiming
  production restore enablement.

## Acceptance Criteria

- The four lanes persist independent `afterSessionId`, cycle, and revision
  values outside every session volume.
- A non-null next cursor advances within the current cycle; a null next cursor
  wraps to null and increments the cycle.
- Exact transition replay performs no second state change. A stale revision,
  cycle, prior cursor, transition ID, or digest fails closed.
- Cursor state advances only after the runner consumes a one-use receipt from
  the exact real recovery service, bound to the lane, input cursor, and limit.
  Failure or abort preserves already-settled lanes and leaves the current or
  later lanes unchanged except for an explicitly settled aborted receipt.
- A new runner instance resumes from the committed database cursor. Two
  runners cannot regress or cross-write lane state.
- Current-launch recovery remains inventory-only. This slice cannot call
  `runRestore()`, publish a destination, reserve or consume an image, invoke a
  launcher, or reconstruct a writer/stopped-writer capability.
- The complete local test suite and the real PostgreSQL PR integration gate
  pass, followed by the required review workflow and resolved conversations.

## Dependency Order

1. Durable recovery cursor store and one-step runner (this workstream).
2. Provider-backed canonical detach composition.
3. Invocation-gated production restore adapter enablement.
4. Filesystem-image backend, differential backup, and cross-host restore.

## Implementation

- Authority migration version 5 adds independent cursor rows keyed by
  `(recovery_scope_id, lane)` with exact lane, revision/cycle, initial-state,
  transition/digest, and identifier constraints.
- `PostgresRestoreRecoveryCursorStore` lazily initializes a lane, locks it in a
  serializable transaction, advances by exact revision/cycle/prior-cursor CAS,
  recognizes exact transition replay, and resolves commit acknowledgement loss
  by a separate durable readback. A proved not-committed missing initial row may
  safely retry; uncertain absence remains fail-closed.
- The recovery service brands its own batch receipts with module-private weak
  identity. The runner accepts only the exact factory-returned service and
  consumes each receipt once, bound to issuer, lane, input cursor, and limit,
  before it can call the low-level cursor CAS.
- The single-flight runner binds each transition to a canonical SHA-256 over
  scope, lane, expected cursor, limit, and the complete normalized batch. It
  hashes a private null-prototype batch array so post-import prototype
  `toJSON` hooks cannot alter that binding while clean-runtime digest bytes
  remain compatible. Service and runner array copies use captured own-property
  definition rather than assignment or `push`, and the lane table is read by
  own numeric index instead of an inherited iterator. Frozen-record validation
  receives a primitive boolean instead of destructuring an ordinary default
  object, and digest construction uses the startup-captured `createHash`
  intrinsic. The runner processes generation, activation, launch-attempt, and
  current-launch in order, preserving earlier durable progress on later
  failure.

## Validation

- Cursor-store unit coverage exercises initialization, exact CAS, wrap,
  conflict, replay, commit acknowledgement loss, proved not-committed retry,
  malformed rows, and hostile Proxy/prototype inputs.
- Recovery-service and runner unit coverage exercises authentic one-use receipt
  provenance, fixed lane order, abort before work, drained partial abort,
  partial failure, deterministic request binding, post-import Array/Object
  prototype pollution, callback-time iterator and numeric-property pollution,
  inherited non-writable numeric properties, inherited option getters across
  successive runs, callback-time Node builtin export synchronization,
  single-flight admission, and malformed collaborator boundaries.
- Migration executor coverage verifies the checksum-bound version 5 chain and
  upgrade/future-ledger behavior. The PostgreSQL integration gate additionally
  covers the real schema, constraint inventory, four-lane initialization,
  concurrent CAS, and commit-acknowledgement-loss replay.
- The local unit suite passes with the existing live external-auth refresh test
  explicitly skipped. On this host that exact unchanged test independently
  fails because its `fs.watch` monitor receives `EMFILE`; the PR's clean full
  unit and PostgreSQL jobs remain mandatory merge gates.

## Non-Goals

- No detached-production fleet capability or `runRestore()` enablement.
- No provider detach/fence composition.
- No publication, activation, writer launch, or physical backend changes.
- No periodic long-goal snapshot scheduler, differential compression,
  retention, encryption, cross-host transport, auth change, or Git Summary.
