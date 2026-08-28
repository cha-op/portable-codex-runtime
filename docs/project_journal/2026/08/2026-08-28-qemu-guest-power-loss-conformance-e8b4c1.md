---
id: 20260828-e8b4c1
title: QEMU Sudden Guest Power-Loss Conformance
status: completed
created: 2026-08-28
updated: 2026-08-28
branch: wip/linux-ext4-sudden-guest-power-loss
pr:
supersedes: []
superseded_by:
---

# QEMU Sudden Guest Power-Loss Conformance

## Summary

- Added a dedicated root-only Ubuntu 24.04 conformance boundary in which an
  external host controller sends `SIGKILL` to and joins one exact non-
  daemonised QEMU child. The guest receives no signal: its CPU, RAM, and
  device-model state disappear with the QEMU process while the host runner and
  storage remain online.
- Kept the production Linux ext4 capability tuple, checkpoint API, runtime
  authority graph, and launcher admission unchanged.

## Current State

- Setup, armed, and recovery boots use the same `data.raw` regular-file object.
  The harness revalidates its path-bound device, inode, mode, link count, and
  size across all three boots rather than restoring from the crash artefact.
  Device and inode protect object identity, mode and link count protect the
  scoped access/alias policy, and size protects the fixed medium boundary.
  Modification and change times are intentionally excluded because ordinary
  QEMU writes and ext4 journal replay change them without violating those
  properties.
- During the armed boot, the guest writes and fsyncs an exact 4,096-byte valid
  JSONL prefix `P`, fsyncs the containing rollout directory entry `D`, writes
  but does not fsync a synthetic tail, emits one nonce-bound serial ready
  marker, and remains alive. The host controller then sends `SIGKILL` to the
  exact non-daemonised QEMU child and requires its joined exit to be
  `{ code: null, signal: "SIGKILL" }`.
- QMP performs only a `query-block` inspection and requires
  `writeback=true`, `direct=true`, and `no-flush=false`. It never requests
  graceful shutdown, power-down, pause, or quit and is not the crash injector.
- The cold recovery boot on the same `data.raw` object performs ext4 journal
  replay and preserves `P` exactly. The superblock must expose
  `needs_recovery` after the external kill and clear it after the clean recovery
  unmount; the human-readable filesystem state is used only to reject an error
  state. The recovered unsynced tail may contain
  any bytes, including zeros, at any length from zero through the attempted
  length; it need not equal a prefix of the attempted write. It must contain no
  LF, complete JSON value, or abort marker. The production
  `repairStoppedRolloutTails()` primitive then converges the rollout exactly
  to `P`, remains idempotent, and permits one synced valid continuation that
  survives a read-only cold remount.
- Before recovery, the harness fsyncs a separate raw crash artefact, sets it to
  mode `0400`, and records its distinct inode, access policy, size, and SHA-256.
  The artefact is never mounted; every identity and content signal remains
  unchanged after same-medium recovery and repair.
- The dedicated CI job uses QEMU TCG, an explicitly selected installed guest
  kernel and generated initramfs, a private mount namespace, bounded boot and
  harness timeouts, and an `always()` report-only residue gate for scoped QEMU
  processes, mounts, loops, the exact test root, and a separate length-bounded
  QMP control root. The gate reports and fails without signalling a discovered
  PID.

## Safety Boundary

- This proves external-QEMU-SIGKILL sudden guest power loss under the exact
  configured and observed block-cache tuple on healthy host storage. It does
  not simulate host power loss, controller or drive volatile-cache loss, a
  device that lies about acknowledged durability, or verify FUA. The emitted
  evidence records `hostPowerLossClaimed: false`,
  `controllerCachePowerLossClaimed: false`, and `fuaVerified: false`.
- It does not prove whole-filesystem durability, a filesystem freeze/flush
  barrier, a general guest or writer containment mechanism, or automatic
  stale-writer fencing. Its durable application boundary is only `P` and `D`.
- It emits no checkpoint descriptor, performs no production catalogue or
  authority mutation, publishes no artefact, and supplies no production
  crash-prefix composition, launcher admission, distribution, or remote
  transport capability.

## Follow-up

- Keep host/controller/drive cache-loss evidence, production crash-prefix
  composition, automatic stale-writer fencing, and distribution as separately
  scoped work.
- Keep Git Summary deferred as read-only user context rather than checkpoint or
  recovery authority.

## Validation

- `node --check integration/linux-ext4-sudden-guest-power-loss-conformance.mjs`
  passed.
- The local non-privileged harness tests passed with four passes; the explicit
  Linux/QEMU case skipped without its opt-in environment.
- The guest C source passed the local fallback warning-clean object compile;
  both initramfs scripts passed `bash -n` and `sh -n`. Static Linux linking and
  the generated-initramfs content gate remain owned by Ubuntu CI.
- Project-journal validation and `git diff --check` passed.
- The privileged three-boot QEMU path is owned by the dedicated Ubuntu 24.04 CI
  job because the local macOS host does not provide the Linux guest kernel,
  initramfs, ext4, and QEMU runtime used by that gate.

## Evidence

- Harness: `integration/linux-ext4-sudden-guest-power-loss-conformance.mjs`
- Guest fixture:
  `integration/fixtures/linux-ext4-sudden-guest-power-loss/guest.c`
- CI gate: `.github/workflows/test.yml`
- Recovery primitive: `src/rollout-tail-repair.mjs`
- Architecture boundary: `docs/architecture/linux-ext4-physical-backend.md`
