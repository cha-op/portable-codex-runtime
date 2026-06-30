---
id: 20260630-1424ea
title: External Auth Compatibility Probe
status: completed
created: 2026-06-30
updated: 2026-06-30
branch: wip/external-auth-probes
pr:
supersedes: []
superseded_by:
---

# External Auth Compatibility Probe

## Summary

- Added offline and live probes for the experimental app-server
  `chatgptAuthTokens` authentication boundary.

## Current State

- The offline probe verifies capability gating, 401 refresh callbacks, token
  replacement, and ephemeral worker storage with synthetic credentials.
- The live probe completed a real `gpt-5.4` turn using a dedicated managed
  ChatGPT access token and left both the source auth file and worker auth file
  state unchanged.
- Committable evidence is redacted and excludes credentials, complete account
  identifiers, and token-derived fingerprints.

## Next Steps

- Continue the auth-broker and session snapshot work as separate workstreams.

## Evidence

- `evidence/live-external-auth.json`
- `npm test`
- `npm run probe:external-auth:live`
