---
id: 20260701-8b2e41
title: Auth Refresh Authority Spike
status: completed
created: 2026-07-01
updated: 2026-07-01
branch: wip/auth-refresh-authority-spike
pr: 2
supersedes: []
superseded_by:
---

# Auth Refresh Authority Spike

## Summary

- Proved that a managed-auth Codex app-server can proactively rotate ChatGPT
  credentials without a normal model turn.
- Preserved the successful adapter, process-local singleflight behavior, atomic
  canonical promotion, tests, documentation, and redacted live evidence.

## Current State

- The successful method is stable v2 `account/read {refreshToken:true}`.
- Codex writes only an isolated staging auth home; verified state is atomically
  promoted into the dedicated authority home.
- Lock loss after refresh starts and lost promotion acknowledgements fail
  closed, preserve durable recovery sentinels, and block automatic token reuse.
- Successful results are sourced from a lock-protected canonical reread rather
  than the staged credential.
- A separate worker validation consumed the refreshed access token through
  `chatgptAuthTokens` without persisting worker auth state.
- Distributed leases, encrypted storage, durable CAS, and broker APIs remain in
  the later Auth Broker MVP workstream.

## Next Steps

- Characterize interrupted and killed turn recovery in the next serial pull
  request.
- Use this experiment as the refresh adapter evidence for the Auth Broker MVP.

## Evidence

- `docs/experiments/auth-refresh-authority.md`
- `evidence/live-auth-refresh-authority.json`
- `npm test`
- `CODEX_ALLOW_AUTH_MUTATION=1 npm run probe:auth-refresh:live`
