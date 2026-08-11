---
id: 20260811-4f8c2d
title: Detached Restore Foreground Composition
status: completed
created: 2026-08-11
updated: 2026-08-11
branch: wip/detached-restore-foreground-composition
pr:
supersedes: []
superseded_by:
---

# Detached Restore Foreground Composition

## Summary

Completed production-neutral phase A of detached restore enablement: one
caller-persisted stable root plan, an invocation-time default-deny fleet gate,
and one foreground facade that composes the already-durable authority
boundaries under the shared restore-lifecycle lease. The production checkpoint
adapter is unchanged and `runRestore()` remains fixed fail-closed until phase-B
runtime assembly and end-to-end coverage land.

## Current State

- `createPostgresDetachedRestorePlan()` canonicalizes one exact outer restore
  request plus source checkpoint artefact, detached destination, stable
  `captureCreatedAt`, detach mode, target holder, image plan, and lease
  duration. It freezes the normalized request and plan and binds them with one
  `planSha256`.
- Domain-separated SHA-256 roles derive stable renewal, safety-capture
  operation/artifact/checkpoint, target generation, destination-isolation
  proof, detach, activation, and launch-attempt IDs. The logical launcher and
  checkpoint authority continue to mint the formal stop-operation ID and
  capture-attempt UUID because those existing authorities have no ID injection
  seam.
- `sourceArtifactDirectory` and `sourceArtifactOwnedRoot` name the checkpoint
  selected by the outer restore request. They are not the fresh safety-capture
  path. The capture backend resolves that separate path from the derived
  capture identities.
- Fresh foreground work requires the exact opaque detached-production
  confirmation capability on every invocation. Exact authority readback for
  matching already-durable work precedes that default-deny decision, so a gate
  closure does not by itself strand an already-materialized V3 stop-to-capture
  handoff or later typed operation. A stop that may have started before the
  atomic handoff remains blocked and cannot be redispatched.
- One shared lifecycle lease covers the ordered foreground topology:
  renewal-before-stop -> V3 stop/prepared capture -> generation V1 publication
  -> selected release or force-fence detach -> activation V2 -> prepared
  launch. Detach mode never falls back automatically.
- A cold retry can reconstruct one already-reserved generation cut only when
  the current active pointer is the exact revision-zero `prepared` generation
  operation, its request and reservation identities match the stable plan,
  its session revision follows the direct predecessor, its operation and
  reservation share the current session's database timestamp, and its
  `lastOperation` is the immediately preceding committed safety-capture
  terminal. The facade then retries the same claim without a second reserve
  or publication. An unproved or non-direct predecessor remains fail-closed.
- Only a `prepared` launch attempt may mint an image reservation and enter the
  prepared-launch dispatch path. `starting`, `uncertain`, and `committed`
  attempts use no-relaunch reconciliation and never prepare another image.
  A historical committed launch may remain authoritative after a later active
  or terminal operation replaces the session anchors only when the immutable
  launch operation, reservation, current `document.launch` pointer, and
  descendant session identity together prove the original transition.
- Factory admission requires the nested per-operation guard pool to be
  distinct from both lifecycle pools before any connection is acquired. This
  prevents a max-one foreground pool from self-deadlocking while its shared
  lease waits for a nested exclusive operation.
- After the prepared launch is verified as started, the facade returns the
  exact restore-generation publication callback completion through the
  lifecycle guard's callback-scoped completion carrier. The launch result is
  evidence, not the public return value. Collaborators stay separately branded
  or structurally validated; serialized IDs and durable rows do not become
  opaque capabilities.

## Retry and Uncertain Outcomes

- Retry is caller-driven. The caller must durably retain and resubmit the same
  stable plan; phase A adds no top-level saga row and performs no autonomous
  cross-stage restart advancement.
- A renewal committed before the V3 stop/capture handoff is not yet a typed
  durable continuation: the current public authority cannot read that renewal
  by operation ID and bind its original expected session and request to the
  plan. A cold retry at this cut therefore fails closed. The writer remains
  running, and phase B may add a typed renewal read if this cut must survive a
  later closure of fresh admission.
- Before a physical dispatch, exact absence or a safe prepared state may admit
  the matching transition. Once stop, fresh capture, generation publication,
  detach, provider activation, or launch may have crossed its dispatch
  boundary, retry delegates only to that subsystem's existing exact readback,
  finalizer replay, committed-only verification, or no-relaunch reconciliation.
- A V3 stop retained as `starting` or `uncertain` cannot be physically stopped
  again. A capture whose fresh dispatch may have committed cannot republish;
  `starting` or `uncertain` capture recovery is source-free and committed-only.
  An exact prepared generation reserve whose direct safety-capture predecessor
  is still proved retries the same claim without another reserve; the sole
  publication remains gated on a definite claim grant. Claim or publication
  acknowledgement loss may replay exact committed readback or finalization,
  but never a second publication. Any crossed request, active pointer,
  revision, timestamp, reservation, or predecessor fails closed.
- Detach ambiguity follows the selected writer-detach contract and cannot
  switch from release to force-fence. Activation ambiguity uses the existing
  typed recovery coordinator. Only a `prepared` launch mints an image;
  `starting`, `uncertain`, and `committed` attempts reconcile from supervisor
  evidence without image preparation or relaunch.
- A blocked earlier stage may therefore keep the caller-driven workflow
  blocked. The facade does not infer success from a path, journal record,
  generation row, attachment, measurement, or process inventory alone.

## Lease Boundary

The facade renews the current writer lease before stop, but that does not make
the lease timeless. On the fresh path, the successful renewal's PostgreSQL
`authorityNow`, not worker `Date.now()`, supplies `now` to local capture
preparation as its database-authoritative timestamp. Lease expiry is
deliberately not a stop-claim gate: that claim advances only through the exact
session identity, claimant token, and capture intent. Generation V1 later
reads the current database clock independently in its dispatch-claim
transaction; the preceding generic reserve does not read that clock. The
stable `captureCreatedAt` and derived identities remain unchanged if capture
is slow, but expiry discovered after capture fails closed and authorizes
neither a second stop nor a second fresh capture. Activation V2 creates the
prepared launch under another bounded lease; expiry before the launch claim
likewise fails closed and never authorizes relaunch. Phase B must choose an
operational lease budget/deadline and explicit recovery policy for long
capture and activation windows.

## Next Steps

1. Assemble the plan resolver, capture-only backend, foreground facade, three
   distinct guard pools, and bounded recovery scheduler in the production
   runtime.
2. Add complete restart and acknowledgement-loss coverage for the assembled
   adapter, including every no-second-stop, capture, publication, activation,
   and launch boundary.
3. Enable the production checkpoint adapter's `runRestore()` only after those
   phase-B gates pass.

## Non-Goals

- No production checkpoint-adapter or scheduler assembly in phase A.
- No change to the fixed fail-closed production `runRestore()` stub.
- No autonomous durable saga, takeover lease, or guessed cross-stage recovery.
- No new PostgreSQL schema or change to the formal stop/capture-attempt ID
  authorities.
- No concrete Podman, Docker, ext4, filesystem-image, NFS, or cross-host
  backend.
- No Git Summary implementation.

## Evidence

- `src/postgres-detached-restore-plan.mjs`
- `src/postgres-detached-restore-foreground-composition.mjs`
- `test/postgres-detached-restore-plan.test.mjs`
- `test/postgres-detached-restore-foreground-composition.test.mjs`
- Existing V3 durable stop/capture, generation V1 publication, writer-detach,
  activation V2, prepared-launch, lifecycle-guard, and no-relaunch recovery
  test suites.
