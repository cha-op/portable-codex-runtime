---
id: 20260805-5b4c91
title: Durable Stop Terminal Readback
status: completed
created: 2026-08-05
updated: 2026-08-05
branch: wip/durable-stop-finalize-readback
pr:
supersedes: []
superseded_by:
---

# Durable Stop Terminal Readback

## Summary

Recover a durably committed writer stop when all bounded finalization replies
remain unavailable or malformed. After the existing three finalization
attempts, the launcher performs one fresh authority read and accepts it only
as the exact current committed terminal transition. Physical stop is never
repeated.

## Protected Property

A stopped-writer capability is issued only when the readback proves all of the
following together:

- the retained request is the current claim-bound request version 2 and both
  the authority result and local digest match its original claim token;
- the operation request, released reservation, committed transition, session
  revision, and terminal `lastOperation` pointer are exact;
- the complete-stop evidence matches the original launch, supervisor,
  process, writer, and stop-operation identities; and
- the committed stop remains the canonical terminal session anchor.

Request, token, evidence, reservation, pointer, or revision drift remains
uncertain and cannot issue a capability.

## Current State

- Normal finalization and exact replay retain their existing bounded path.
- Persistent acknowledgement loss or malformed finalization output may fall
  back to one fresh exact terminal readback after those attempts.
- The recovered receipt is marked `finalized: false` and otherwise preserves
  the same frozen committed-stop result shape used by capture composition.

## Non-Goals

- No historical-successor cleanup or retirement path.
- No retryable stop-admission API change.
- No checkpoint-capture request version 2 or fleet cutover gate; that remains
  the next independent workstream.

## Validation

- Node.js `v24.18.0` syntax checks passed for the launcher and its test file.
- The launcher test file passed 115/115, including persistent acknowledgement
  loss, persistent malformed output, a genuinely non-committed terminal
  readback, and six fail-closed relation-drift cases.
- The launcher plus durable stop/capture composition tests passed together.
- The complete Node test suite passed with exit 0 while Codex was excluded
  from `PATH`; the app-server auth file separately reported 42 passed and 2
  expected Codex-unavailable skips.
- The PostgreSQL integration gate could not start locally because
  `SESSION_AUTHORITY_DATABASE_URL` is unavailable; required PR CI remains the
  integration authority.

## Evidence

- `src/postgres-logical-writer-launcher.mjs`
- `test/postgres-logical-writer-launcher.test.mjs`
