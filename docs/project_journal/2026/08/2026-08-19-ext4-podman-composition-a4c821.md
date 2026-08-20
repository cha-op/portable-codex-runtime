---
id: 20260819-a4c821
title: ext4-to-Podman Attachment Composition
status: completed
created: 2026-08-19
updated: 2026-08-20
branch: wip/ext4-podman-composition
pr:
supersedes: []
superseded_by:
---

# ext4-to-Podman Attachment Composition

## Summary

- Completed the trusted bridge from an initialized ext4 backend's committed
  attachment identity to the Podman writer filesystem authority.
- Added same-process Linux producer evidence: after committed attach and before
  detach, one non-root Node process launches and stops a real rootless Podman
  writer and carries its marker through the clean cross-host transfer.

## Current State

- `createExt4PodmanAttachmentBinding()` constructs the initialized backend and
  filesystem authority from the same driver and provider-state objects. The
  verifier reconstructs the exact attachment from its committed origin
  operation and current storage state before admitting Podman. Its version 2
  persistent-authority contract requires the provider view's
  `currentAttachmentOriginOperationId` to equal that queried operation and
  permits only a legal monotonic storage-revision increase between the origin
  and current otherwise-equal canonical storage state.
- The protected object identity has two explicit layers: the provider's
  committed persistent filesystem/file-handle identity and the driver's
  same-sample runtime `device`/`inode`. Podman compares the latter to its held
  attachment FD and revalidates the live bind. Access policy remains a separate
  check; child-entry, file-content, and timestamp churn are allowed.
- After exact Podman start can have executed, failures in inspection, live-bind
  proof, persistent authority, durable transition, or authority close are
  exposed only as `podman_writer_supervisor_outcome_uncertain`; they never leak
  a conclusive missing or mismatch result. Reconciliation applies the same rule
  once it has observed the exact container running.
- The independent rootless Podman conformance job remains as narrower
  supervisor coverage. The composed producer closes the ext4-to-Podman
  identity gap but does not claim power-loss recovery, automatic stale-writer
  fencing, or one whole-saga generic PostgreSQL deployment run.
- Rootless Podman's pause process retains a real mount-namespace reference
  after the exact container is stopped and removed. The dedicated hosted
  producer now proves its complete container and pod inventories empty, then
  performs a bounded user-wide namespace retirement before ext4 detach and
  makes no later Podman call. This is a conformance-host lifecycle barrier,
  not a generic supervisor operation: a shared UID or engine cannot use it,
  and final quiescence still requires the native loop-detach receipt.

## Follow-up

- Version 3 provider-state adoption now retains and validates every current
  attachment's origin while PostgreSQL supplies permanent operation replay.
  A future streaming adoption contract is still required for version 2 states
  beyond the current 65,535-operation/storage full-array capacity.
- Keep Git Summary deferred; it is not checkpoint or recovery authority.

## Evidence

- Composition: `src/ext4-podman-attachment-binding.mjs`
- Driver and Podman authority extensions: `src/linux-ext4-image-driver.mjs`,
  `src/podman-writer-supervisor.mjs`
- Unit coverage: `test/ext4-podman-attachment-binding.test.mjs`,
  `test/ext4-podman-writer-integration.test.mjs`,
  `test/linux-ext4-image-driver.test.mjs`,
  `test/podman-writer-supervisor.test.mjs`
- Linux producer/consumer coverage: `integration/ext4-podman-writer.mjs`,
  `integration/linux-ext4-physical-backend.mjs`, `.github/workflows/test.yml`
