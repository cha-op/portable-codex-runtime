---
id: 20260702-4d729b
title: Auth Broker MVP
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/auth-broker-mvp
pr: 5
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
  of an exact crashed reservation, exposing its non-secret owner ID through
  recovery metadata while preventing live reservation takeover.
- Enforced the configured token TTL as an authority floor and reserved
  `refresh_in_progress` exclusively for structurally valid durable
  reservations.
- Preserved recovery/integrity classifications during reservation conflict
  rereads and made lock-release failure dominate retryable operation errors
  without discarding allowlisted commit-state evidence or permitting another
  broker mutation retry.
- Classified canonical symlink replacement as non-retryable invalid auth state
  instead of a transient storage outage.
- Required a successful held-directory sync before public reads or exact CAS
  replays can publish visible state, preventing a rename without proven
  directory durability from reaching workers or a restarted broker.
- Pinned the authority directory identity and canonical path across
  transactions, classified pre-rename lock replacement as definitely
  uncommitted recovery, and rejected refresh responses that reuse the consumed
  refresh token.
- Bound worker callbacks to the account and user actually issued and required
  explicit crashed-reservation recovery to rotate both source access and
  refresh tokens.
- Failed closed on thrown or non-finite clock readings before TTL decisions.
- Preserved recovery fences through terminal post-dispatch states so ordinary
  login cannot republish either source token after an uncertain refresh.
- Enforced the canonical encrypted-envelope byte limit before temporary-file
  creation, so escape-heavy payloads cannot replace readable state with an
  envelope the store rejects on readback.
- Reused ACL policy evidence only inside one transaction and only across
  unchanged authority metadata or explicit ancestor fencing phases, removing
  repeated subprocess amplification without weakening identity and commit
  boundary checks.
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
