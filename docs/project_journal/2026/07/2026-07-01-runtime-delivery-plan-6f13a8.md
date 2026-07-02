---
id: 20260701-6f13a8
title: Portable Runtime Delivery Plan
status: active
created: 2026-07-01
updated: 2026-07-02
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
  durable filesystem operation journal, and local stopped-directory
  publication layer are complete.
- The next serial workstream is the same-process stopped-writer capability.
- The complete dependency order and delivery invariants are recorded in
  `docs/architecture/runtime-delivery-plan.md`.

## Next Steps

- Implement the same-process stopped-writer capability, then compose it with
  publication and canonical fence authority in the stopped-directory backend
  conformance workstream.

## Evidence

- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/stopped-directory-publication.md`
- `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`
