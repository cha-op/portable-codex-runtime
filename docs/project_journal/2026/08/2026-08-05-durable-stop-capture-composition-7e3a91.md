---
id: 20260805-7e3a91
title: Durable Stop-to-Clean-Capture Composition
status: completed
created: 2026-08-05
updated: 2026-08-05
branch: wip/durable-stop-capture-composition
pr: https://github.com/cha-op/portable-codex-runtime/pull/29
supersedes: []
superseded_by:
---

# Durable Stop-to-Clean-Capture Composition

## Summary

Added the same-process boundary joining one prepared clean-capture tuple, one
physical writer stop, the exact committed `writer-launch-stop-v1` transition,
one opaque stopped-writer capability, and one capture dispatch. Local writer
identity is retired only after confirmed capture success. Production restore,
detached-destination activation, and bounded restart recovery remain closed.

## Current State

- The snapshot core separates deterministic preparation from one-use dispatch,
  preserving exact attachment, checkpoint, and mutation-request object
  identity across stop and backend capture.
- The launcher derives its stop operation ID from the complete canonical
  capture tuple plus launch-attempt ID, persists and finalizes the durable stop,
  validates a stop-specific committed transition proof, and returns exact
  frozen `{capability, evidence, resolution, stop}`.
- The launcher retains the first exact stop operation input. Later calls use
  `reconcileOperation()` and proceed only from `prepared`, or from `starting`
  with the same record's claim-attempt witness while it still proves the
  coordinator and physical stop have not begun. It never repeats claim for
  known `starting` state or synthesizes operation timestamps from a session.
- The composition independently derives the same stop ID from its prepared
  tuple before it accepts any receipt. A valid receipt for a different request
  or checkpoint is rejected before capture.
- `retireStoppedWriter(resolution)` accepts only the retained exact writer
  resolution and releases launcher indexes only after capture. A mistakenly
  returned native Promise is rejection-observed without awaiting it and is
  still classified as retirement uncertainty.
- The canonical same-process stopped-writer coordinator retains a per-session
  launch-exclusion count across running, stopped, issued, consuming, consumed,
  revoked, and uncertain records. Both launch preflight and direct writer
  registration enforce that count, so sharing the exact coordinator blocks
  successor launch across backend and storage slots until retirement.
- Public envelope validators reject ordinary and revoked proxies with their
  fixed contract error classification before array introspection or dispatch.
- The retained migration-version-3 operation-ID registry and restore schema
  remain documented after the restore-composition revert; this work does not
  reinstate the reverted restore facade or callback contract.

## Protected Property

Only a complete canonical `{attachment, checkpoint, request}` tuple bound to
the current launch attempt may name the stop that authorizes its capture. The
launcher must definitely claim the durable stop before invoking physical stop
once, then validate the typed result, released reservation, direct terminal
session successor, cleared launch, and exact complete-stop evidence before a
capability can be issued.

Stop, finalization, capability, capture, or retirement ambiguity retains the
durable and process-local blockers. It cannot repeat physical stop, replace the
capability, reconstruct a writer handle, or release the retained identity.
The protected access policy also forbids a physical successor launch for the
same session while that retained identity remains. This is same-process
composition inside one canonical coordinator authority domain, not restart
recovery or a cross-process or cross-host fence.

## Next Steps

1. Publish restore into an independent detached destination, verify its
   committed object identity, obtain provider-backed attachment evidence, and
   atomically activate that attachment only after the old writer is stopped,
   fenced, and detached.
2. Compose bounded generation, prepared-launch, active-attempt, and
   current-launch recovery without relaunching or reconstructing opaque image
   or writer capabilities.
3. Wire the complete protocol into production `runRestore()` only after all
   ambiguous boundaries remain fail-closed.
4. Implement the later filesystem-image backend, differential export,
   retention, and cross-host recovery verification.

## Non-Goals

- No reinstatement of the reverted restore publication facade.
- No detached-destination attachment activation.
- No bounded restart recovery service.
- No production `runRestore()` enablement.
- No concrete Podman/Docker launcher or filesystem-image backend.
- No Git Summary implementation.

## Validation

- On Node.js `v24.18.0`, `node --check` passed for the stopped-writer
  coordinator, authority, launcher, snapshot core, and new composition modules.
- The five focused stopped-writer coordinator, launcher, snapshot-core,
  composition, and operation-kernel test files passed with exit 0 and no
  failures. They include direct same-session cross-backend/storage registration
  exclusion, concurrent capability consumption, exact prepared/starting stop
  replay, and rejection of `starting` without the retained record's claim
  witness.
- `test/stopped-directory-backend.test.mjs` passed with 91 dot-reporter test
  markers and no failures.
- The complete unit suite passed with exit 0 when it skipped only
  `chatgptAuthTokens refreshes after 401 without writing auth.json`.
- That exact unskipped live-auth test failed with the host-wide
  `EMFILE: too many open files, watch` condition in both this worktree and the
  unmodified `cfe96a7` baseline, so it is retained as an environmental
  limitation rather than reported as passing.

## Evidence

- `src/postgres-durable-stop-capture-composition.mjs`
- `src/postgres-logical-writer-launcher.mjs`
- `src/postgres-session-authority.mjs`
- `src/session-snapshot-core.mjs`
- `src/stopped-writer-capability.mjs`
- `test/postgres-durable-stop-capture-composition.test.mjs`
- `test/postgres-logical-writer-launcher.test.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/session-snapshot-core.test.mjs`
- `test/stopped-writer-capability.test.mjs`
