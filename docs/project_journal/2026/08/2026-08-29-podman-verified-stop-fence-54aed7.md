---
id: 20260829-54aed7
title: Podman Verified-Stop Fence Provider
status: completed
created: 2026-08-29
updated: 2026-08-29
branch: wip/atomic-crash-capture-stale-writer-fence
pr: https://github.com/cha-op/portable-codex-runtime/pull/64
supersedes: []
superseded_by:
---

# Podman Verified-Stop Fence Provider

## Summary

- Added a private verified-stop fence for the exact local rootless Podman
  writer incarnation bound to one ext4 attachment and force-fence operation.
- Added source-free provider reconciliation and a durable PostgreSQL binding
  resolver so process restart never turns an authority `starting` or
  `uncertain` state into another logical fence dispatch.
- Added a dedicated force-fence V2 composition that commits the authenticated
  provider proof through the existing atomic capture handoff. The resulting
  capture remains `prepared` and active; this slice does not dispatch or
  release it.

## Current State

- Fresh physical dispatch starts only from the exact supervisor `started`
  record. It durably records the force-fence operation as the stop identity
  before Podman stop, joins the container lifecycle, requires exact stopped
  inspection with no container PID, persists revision 4, removes the exact
  container, and proves both anchored-name and full-ID inventories empty.
- Reconciliation cannot initiate a stop from `started`. It may continue only
  the same revision-3 `stopping` operation or revalidate revision 4 through
  idempotent removal and both absence observations. Missing, crossed,
  unreadable, or otherwise ambiguous state remains `unknown`.
- A dedicated PostgreSQL reader resolves the V2 force-fence operation ID to
  its exact pre-fence `ATTACHED` snapshot and claimed fence request. The
  resolver then reads the committed launch under the same durable supervisor
  state-owner binding and projects only the exact launch pointer, original
  supervisor request/result, and state-owner ID needed by the provider.
- The provider recomputes the launch attachment, lease, measured-image and
  terminal-result digests, container name, request digest, process and writer
  incarnations, and start proof before accepting supervisor state. Its opaque
  fence proof additionally binds the complete force-fence request and terminal
  revision-4 identity.
- The V2 composition preserves reserve, typed claim/preclaim, provider outside
  PostgreSQL, exact finalization, and whole-handoff proof validation. Existing
  or ambiguous authority operations never receive a second provider dispatch;
  exact committed provider readback may finish only the original handoff.

## Protected Property

- The exact `(supervisorId, stateOwnerId, launchAttemptId, containerId,
  processIncarnationId, writerIncarnationId)` writer incarnation no longer has
  its supervised `/session` write access to the exact source attachment.
- Object/process identity is protected by the committed launch relation,
  supervisor request digest, complete container ID, derived incarnation IDs,
  durable state-owner binding, and exact revision-4 tombstone.
- Access revocation is protected by joined stopped inspection, `Pid=0`, exact
  container removal, and independent anchored-name and full-ID absence
  inventories. A missing pathname, lease expiry, database epoch, lifecycle
  label, or unbound PID exit is not a substitute.
- Benign lease-expiry extension does not change the selected lease identity:
  the fence compares the stable session/lease/holder/epoch tuple while the
  original launch-time lease digest remains bound to the launch pointer.

## Safety Boundary

- This is a host-local supervised-writer fence. It does not revoke a writer on
  another host or Podman engine, an administrator-created or ordinary host
  process, or a caller that bypasses the trusted launcher.
- It does not prove loop or block-device isolation, ext4 content stability at
  the stop instant, host/controller/drive volatile-cache loss, FUA, or
  whole-filesystem freeze and flush.
- The assembled ext4 backend and public deployment remain
  `fencing: "manual"`. The verified-stop adapter and V2 composition are private
  opt-in surfaces and do not widen production capability discovery.
- Success leaves the exact atomic capture operation in `prepared` as the
  active blocker. No snapshot provider runs, no artifact exists, no blocker is
  released, no tail is repaired, and no higher-epoch successor is admitted.

## Follow-up

- Dispatch and reconcile the exact prepared capture through the durable LVM
  provider, then release the blocker only from exact committed artifact
  evidence.
- Repair the captured tail only on a separate writable generation before
  admitting a successor under a strictly higher epoch and new lease.
- Keep remote/storage-native fencing, cache-loss evidence, distribution,
  compression, encryption, retention, registry trust, and transport separate.

## Evidence

- Storage contracts: `src/session-storage-contracts.mjs`
- Podman supervisor: `src/podman-writer-supervisor.mjs`
- Verified-stop provider: `src/podman-ext4-verified-stop-fence-provider.mjs`
- PostgreSQL binding resolver:
  `src/postgres-podman-verified-stop-fence-binding.mjs`
- V2 composition:
  `src/postgres-writer-force-fence-atomic-capture-composition.mjs`
- Authority: `src/postgres-session-authority.mjs`
- Architecture: `docs/architecture/atomic-crash-capture-extension.md`

## Validation

- `node --test`: 3,612 tests; 3,592 passed and 20 platform-gated tests
  skipped. The complete pass ran outside the filesystem sandbox because the
  sandboxed run exhausted its file-watcher allowance in one unchanged auth
  monitor test; that exact test passed outside the sandbox before the complete
  rerun.
- `node --test test/session-storage-contracts.test.mjs`: 62 passed.
- `node --test test/podman-writer-supervisor.test.mjs`: 56 passed and 12
  Linux-only tests skipped.
- `node --test test/postgres-session-operation-kernel.test.mjs`: 303 passed.
- `node --test test/postgres-podman-verified-stop-fence-binding.test.mjs`: 15
  passed.
- `node --test test/postgres-writer-force-fence-atomic-capture-composition.test.mjs`:
  12 passed.
- `node --check` passed for every changed or added JavaScript source,
  integration, and test entry point.
- The real PostgreSQL and rootless Podman integration jobs require their CI
  service URL, digest-pinned image, executable, and Linux test-root inputs;
  those inputs are unavailable on this local host and remain PR CI gates.
