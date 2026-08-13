---
id: 20260813-4b7c2e
title: Final Public Restore Backend
status: completed
created: 2026-08-13
updated: 2026-08-13
branch: wip/final-public-restore-backend
pr:
supersedes: []
superseded_by:
---

# Final Public Restore Backend

## Summary

- Completed the immutable public checkpoint backend for the assembled
  PostgreSQL detached-restore runtime.
- Replaced the caller-supplied publication callback at the serving boundary
  with the stopped-directory backend's fixed version 3 fresh-versus-committed
  publication router.
- Kept the filesystem/ext4 physical implementation as the next independent
  backend slice.

## Current State

- Runtime assembly first constructs one private capture backend. The durable
  stop/capture composition, writer-detach composition, activation coordinator,
  and foreground restore composition continue to use that exact backend and
  its operator/provider extensions.
- After the foreground composition exists, runtime assembly constructs a
  second immutable `StoppedDirectoryBackend`. Its capture operations reuse the
  original mutation authority, while its restore authority is fixed to the
  version 3 foreground composition. No method is replaced after construction,
  and no mutable placeholder closes the construction cycle.
- Runtime, controller, and deployment expose only a checkpoint facade with
  metadata, `captureCheckpoint()`, and `restoreCheckpoint()`. The five raw
  lifecycle mutations, capture reconciliation, prepared-capture continuation,
  restore-attachment dispatch, and attachment reconciliation remain private to
  the runtime graph because exposing them at serving ingress would bypass their
  durable coordinator or control/operator-plane admission.
- The runtime controller and deployment expose the public backend through
  receiver-preserving admission wrappers. Every admitted backend Promise is
  retained by the controller's in-flight ledger. Stop closes new ingress and
  starts controller drain plus all nineteen physical-settlement stops without
  awaiting one before starting the others. It closes the four PostgreSQL pools
  only after both drain groups settle.
- Production callers invoke `deployment.backend.restoreCheckpoint(request)`.
  They cannot supply the internal two-argument foreground callback. Fresh
  generation publication can call only `publishRestoreDestination()`;
  committed recovery can call only `verifyCommittedRestoreDestination()`
  without source-artifact paths and must return an exact replay.
- The raw runtime remains a low-level assembly capability, but its backend
  projection is already checkpoint-only. Controller/deployment additionally
  own readiness and in-flight drain; callers must not co-serve the raw runtime
  beside those lifecycle owners.

## Safety Boundary

- The adapter does not mint, persist, copy, or reconstruct a physical dispatch
  grant. Stop, capture, generation, detach, activation, and launch retain their
  existing PostgreSQL claim, readback, reconciliation, and no-fallback rules.
- A deadline, abort, late result, settlement-grace breach, path, publication
  record, image measurement, or durable operation identifier never authorizes
  another mutating dispatch.
- Trusted read-only observations may repeat in a distinct recovery attempt,
  but they cannot promote themselves into a generation, attachment, or launch
  grant.
- This slice does not claim filesystem object durability beyond the already
  injected physical collaborator contracts. The concrete filesystem/ext4
  backend remains separate.

## Non-Goals

- No ext4, filesystem-image, reflink, container-runtime, differential export,
  compression, content-addressed storage, encryption, retention, or cross-host
  implementation.
- No new schema, autonomous top-level saga, retry authority, operation ID, or
  recovery lane.
- No public exposure of raw foreground callbacks, physical bindings,
  settlement stops, fatal-shutdown hooks, pools, authorities, or coordinators.

## Next Steps

- Implement the filesystem/ext4 physical backend and its crash, fencing,
  publication, launch, and cross-host conformance evidence.
- Follow with differential export and the remaining storage features.

## Evidence

- `src/postgres-detached-restore-runtime-composition.mjs`
- `src/postgres-detached-restore-runtime-controller.mjs`
- `src/postgres-detached-restore-deployment.mjs`
- `src/session-storage-contracts.mjs`
- `src/session-snapshot-core.mjs`
- `src/stopped-directory-backend.mjs`
- `test/postgres-detached-restore-runtime.test.mjs`
- `test/postgres-detached-restore-runtime-controller.test.mjs`
- `test/postgres-detached-restore-deployment.test.mjs`
- `test/session-storage-contracts.test.mjs`
- `test/session-snapshot-core.test.mjs`
- `integration/postgres-session-authority.mjs`
- `docs/architecture/session-runtime-authority.md`
- `docs/architecture/runtime-delivery-plan.md`
