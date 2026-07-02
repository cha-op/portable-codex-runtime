---
id: 20260701-6f13a8
title: Portable Runtime Delivery Plan
status: active
created: 2026-07-01
updated: 2026-07-01
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
  and the auth broker MVP are complete.
- The next serial workstream is the snapshot and restore core.
- The complete dependency order and delivery invariants are recorded in
  `docs/architecture/runtime-delivery-plan.md`.

## Next Steps

- Implement quiescing, filesystem snapshots, manifests, restore, and same-image
  thread resume verification independently of auth state.

## Evidence

- `docs/architecture/runtime-delivery-plan.md`
- `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`
