---
id: 20260702-4d729b
title: Auth Broker MVP
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/auth-broker-mvp
pr:
supersedes: []
superseded_by:
---

# Auth Broker MVP

## Summary

- Added an AES-256-GCM encrypted canonical auth-state store with durable
  uint64 generations, idempotent commit IDs, CAS, key rotation, atomic
  publication, recovery gates, and secret-safe public results.
- Added a broker state machine for TTL-aware grants, process-local refresh
  single-flight, committed-generation publication, explicit reauthentication,
  recovery blocking, and exact Codex external-token payloads.
- Added exact commit reconciliation, JWT metadata continuity checks,
  caller-specific TTL isolation, durable pre-dispatch recovery reservations,
  canonical filesystem identity, and private last-delivered worker credential
  tracking so stale callbacks do not consume another refresh token.
- Separated ordinary credential installation from supervisor-fenced recovery
  of an exact crashed reservation, preventing live reservation takeover.
- Kept refresh tokens, ID tokens, raw auth JSON, encryption keys, and authority
  paths outside worker protocol responses and session volumes.

## Current State

- Workers can receive only an access token, account ID, and plan type through
  the pinned experimental `chatgptAuthTokens` boundary.
- A token candidate is never returned before encrypted CAS and canonical
  reread.
- OAuth dispatch never begins before a durable credential-free reservation blocks
  reuse of the old refresh-token state.
- The MVP assumes a supervisor-enforced singleton broker process and a trusted
  private authority directory.
- The verified `account/read` choreography is represented by an injected
  adapter boundary; production rootless refresh containment remains deferred.

## Next Steps

- Implement snapshot and restore core independently of auth state.
- Later replace the singleton file coordinator with distributed broker
  leadership and transactional storage when HA is required.

## Evidence

- `docs/architecture/auth-broker.md`
- `src/encrypted-auth-state-store.mjs`
- `src/auth-broker.mjs`
- `test/encrypted-auth-state-store.test.mjs`
- `test/auth-broker.test.mjs`
- Pinned Codex source commit `db887d03e1f907467e33271572dffb73bceecd6b`
