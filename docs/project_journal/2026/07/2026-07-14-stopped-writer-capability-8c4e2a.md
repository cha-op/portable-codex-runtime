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
  trusted callback joins the writer boundary; generator-shaped callbacks or
  results cannot silently synthesize success.
- Public inputs reject hostile proxies and accessors before dispatch. Public
  errors are fixed, frozen, non-retryable, and omit collaborator details and
  private binding data.
- Callback results cannot trigger a second stateful thenable assimilation after
  successful consumption. Module-captured intrinsics reject every proxy
  traversed before the nearest `then` descriptor and reject accessor or
  callable `then` values while preserving non-callable data descriptors;
  violations become terminal uncertainty before success is recorded.
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
