---
id: 20260714-8c4e2a
title: Same-Process Stopped-Writer Capability
status: completed
created: 2026-07-14
updated: 2026-07-14
branch: wip/stopped-writer-capability
pr: 10
supersedes: []
superseded_by:
---

# Same-Process Stopped-Writer Capability

## Summary

- Added an in-memory coordinator that converts one trusted writer stop into one
  authenticated, one-use snapshot capability.
- Kept authority in same-process object identity while binding it to the exact
  process and writer incarnations, complete attachment, writer fence, and stop
  operation.

## Current State

- Writer and capability authority is held in private `WeakMap` records. JSON,
  structured clones, worker messages, copied fields, and cross-coordinator
  lookalikes cannot reproduce the original object identity.
- Each writer incarnation admits one stop operation, one issued capability,
  and one snapshot callback. Stop failure, revocation, callback failure, and
  reentrant or concurrent consumption are terminal and never re-dispatch the
  callback.
- Stop issuance requires the exact exported confirmation sentinel after the
  trusted callback joins the writer boundary. Its direct return is rejected
  before coordinator-owned thenable assimilation, so generator-shaped or
  custom-thenable results cannot silently synthesize success.
- Attachment and lease authority is retained only from exact, flat, frozen
  snapshots built with coordinator-captured intrinsics before shared contract
  validation. Validator return objects and later caller mutations cannot alter
  the registered binding.
- Public inputs reject hostile proxies and accessors before dispatch. Public
  errors are fixed, frozen, non-retryable, and omit collaborator details and
  private binding data.
- Callback results cannot trigger coordinator-owned custom thenable
  assimilation. Before the first `await`, module-captured intrinsics accept
  only descriptor-safe non-Promise values or branded Promises whose nearest
  `constructor` data descriptor names the captured Promise intrinsic. Promise
  subclasses, cross-realm Promises, and accessor or foreign constructors must
  be normalized and owned by the trusted backend callback. The descriptor-only
  `then` check runs again after settlement so the async return cannot perform a
  second stateful assimilation after recording success; violations become
  terminal uncertainty.
- Attachment slots use nested intrinsic `Map` lookups over validated primitive
  IDs rather than an observable serialized composite key, so inherited
  `Array.prototype.toJSON` or `Object.prototype.toJSON` mutations cannot bypass
  single-writer or fencing checks.
- Coordinators serve a finite issuer scope rather than a process-wide stream of
  ephemeral IDs. Terminal `dispose()` succeeds only after every writer safely
  retires, permanently closes the instance, and releases retained slot, writer,
  and capability containers. Uncertain writers can never retire or satisfy
  disposal; after external canonical fencing and teardown, their bounded owner
  abandons the coordinator and drops all related references.
- `turn/completed`, `ShutdownComplete`, `thread/closed`, thread unsubscribe,
  and rollout flush are not writer-stop proof. Production issuance requires a
  fully joined container, cgroup, or VM writer boundary, or a future Codex
  shutdown that propagates failures and joins every persistence writer.
- The capability remains opaque to the snapshot core and is never persisted in
  journals, descriptors, manifests, or snapshots.

## Next Steps

- PR #11 composes capability consumption with the atomic canonical fence
  recheck, stopped-directory publication, exact operation binding, and durable
  idempotent replay in the backend adapter and conformance suite.
- Keep replay-only uncertain-result reconciliation and a joined Codex writer
  shutdown API as later workstreams.

## Evidence

- `docs/architecture/stopped-writer-capability.md`
- `src/stopped-writer-capability.mjs`
- `test/stopped-writer-capability.test.mjs`
- PR #10
