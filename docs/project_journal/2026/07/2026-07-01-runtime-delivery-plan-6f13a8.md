---
id: 20260701-6f13a8
title: Portable Runtime Delivery Plan
status: active
created: 2026-07-01
updated: 2026-07-23
branch:
pr:
supersedes: []
superseded_by:
---

# Portable Runtime Delivery Plan

## Summary

- The runtime will be delivered as a sequence of independently reviewed and
  squash-merged pull requests.
- Pull requests remain serial; source research, test design, and focused review
  may run in parallel within the active pull request.

## Current State

- The external `chatgptAuthTokens` consumer boundary is already proven.
- Auth refresh authority, interrupted-turn recovery, session storage contracts,
  the auth broker MVP, snapshot and restore core, stopped-tree primitives,
  durable filesystem operation journal, stopped-directory publication,
  stopped-writer capability, backend composition, and committed capture
  reconciliation, pinned-executable resume evidence, and offline rollout-tail
  repair before writable recovery are complete through PR #13.
- The PostgreSQL serializable authority foundation, initial durable schema,
  real concurrency coverage, and bounded OCI/Docker runnable-image reservation
  are complete in PR #14 without claiming session lifecycle or container
  launch.
- The complete dependency order and delivery invariants are recorded in
  `docs/architecture/runtime-delivery-plan.md`.

## Next Steps

- Implement the production session lifecycle, lease, reservation, catalogue,
  fencing, attachment, and launcher-admission transitions behind the validated
  backend seam.

## Evidence

- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/stopped-directory-publication.md`
- `docs/project_journal/2026/07/2026-07-15-pinned-executable-resume-tail-repair-9d813d.md`
- `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`
