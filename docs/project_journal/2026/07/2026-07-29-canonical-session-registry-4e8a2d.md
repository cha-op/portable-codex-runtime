---
id: 20260729-4e8a2d
title: Canonical Session Registry
status: completed
created: 2026-07-29
updated: 2026-07-29
branch: wip/session-authority-registry
pr:
supersedes: []
superseded_by:
---

# Canonical Session Registry

## Summary

- Added the first production session-authority slice on the existing
  PostgreSQL serializable foundation.
- Kept registration separate from lease, attachment, fence, catalogue, and
  launcher authority so identity persistence cannot be mistaken for writer
  admission.

## Current State

- `PostgresSessionAuthority` registers one immutable session manifest, storage
  reference, and backend capability set as revision zero in the initial
  `DETACHED` lifecycle.
- Exact registration replay returns the original row without changing revision
  or timestamps. Any different identity under the same session ID fails closed
  without overwriting canonical state.
- Readback validates the complete relational and JSON document shape before
  returning a deep-frozen snapshot.
- Unit coverage uses deterministic transaction doubles. The PostgreSQL
  integration gate covers ordinary readback plus concurrent identical and
  conflicting registration with explicit synchronization.
- Registration does not allocate an epoch or lease, attach storage, invoke a
  provider, or authorize a container launch.

## Next Steps

- Implement the durable operation and reservation kernel, including exact
  request replay, conflict exclusion, and retained ambiguous outcomes.
- Decide the documented conflict classes explicitly before relying on the
  initial schema's current per-session active-operation and reservation
  uniqueness.

## Evidence

- `src/postgres-session-authority.mjs`
- `src/session-storage-contracts.mjs`
- `test/postgres-session-authority.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
