---
id: 20260810-a6e4c2
title: Activation Launch Compatibility
status: completed
created: 2026-08-10
updated: 2026-08-10
branch: wip/restore-activation-launch-compatibility
pr:
supersedes: []
superseded_by:
---

# Activation Launch Compatibility

## Summary

Completed the local protocol slice that admits a target restore generation
between clean capture and detach, preserves the prior activation-v2 topology,
and makes the detached activation-to-prepared-launch handoff executable through
the real PostgreSQL logical launcher. Production restore remains fail-closed.

## Current State

- Historical activation-v2 work keeps the durable backward predecessor chain
  detach to capture to stop. Exact replay and recovery do not depend on the new
  topology gate.
- Fresh work may use detach to generation to capture to stop when the target is
  a committed version 1 restore generation created after the clean capture and
  before detach. The generation's expected last pointer is the capture, and the
  detach operation's expected last pointer is that generation.
- `restoreAttachmentActivationV2GenerationPredecessorFleetCompatible` is a
  separate default-closed startup decision in addition to the existing
  activation-v2 fleet gate. Both must permit a fresh generation-predecessor
  reservation.
- `prepareLaunchIntent()` accepts only a clean canonical version 3 `DETACHED`
  release or force-fence snapshot for this path. It does not read or mutate
  authority state, reserve a launch attempt, consume the image reservation, or
  invoke the launcher.
- Activation finalization atomically installs the provider-proven attachment
  and materializes the exact prepared launch. `runPreparedLaunch()` validates
  that activation-produced relation, claims the existing attempt, and invokes
  the physical launcher once. Exact finalizer and launcher replay create no
  second launch reservation or physical launch.
- Real-PostgreSQL integration coverage follows current launch, durable stop,
  committed clean capture, distinct version 1 target generation, release
  detach, activation-v2 finalization with commit-acknowledgement loss, prepared
  launch, and active-launch readback.

## Safety Decisions

- The protected property is the exact durable predecessor identity chain, not
  timestamp order. Each compared `lastOperation` pointer binds one committed
  operation and its expected session revision; attachment, capture, generation,
  and detach identities must agree across their permanent authority records.
- Fleet compatibility for the request schema and compatibility for the new
  durable topology are separate rollout facts. The topology gate therefore
  cannot be inferred from the existing activation-v2 gate.
- A clean detached session is preparation input only. Serialized image
  measurement and launch intent remain correlation data; the opaque one-use
  image reservation is still required at dispatch.
- Activation finalization acknowledgement loss is resolved by exact durable
  replay. The launcher reads the one prepared attempt and never repairs the
  boundary by reserving a replacement.

## Next Steps

1. Add the cross-process foreground/recovery lifecycle guard so prepared launch
   dispatch cannot race scheduled recovery cancellation.
2. Add the bounded production recovery scheduler.
3. Behind the separate detached-production fleet capability, wire publication,
   stop/capture, detach, activation, prepared launch, and recovery through the
   checkpoint adapter before enabling `runRestore()`.

## Non-Goals

- No schema or migration change.
- No production checkpoint-adapter restore wiring or `runRestore()` enablement.
- No recovery scheduler or cross-process lifecycle guard.
- No concrete Podman or Docker launcher.
- No Git Summary implementation.

## Evidence

- `src/postgres-session-authority.mjs`
- `src/postgres-logical-writer-launcher.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/postgres-logical-writer-launcher.test.mjs`
- `integration/postgres-session-authority.mjs`
- `README.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
- `node --check integration/postgres-session-authority.mjs`
- `node --test --test-reporter=dot test/postgres-logical-writer-launcher.test.mjs test/postgres-session-operation-kernel.test.mjs`
- Real PostgreSQL execution remains a required external gate because
  `SESSION_AUTHORITY_DATABASE_URL` is unavailable in this workspace.
