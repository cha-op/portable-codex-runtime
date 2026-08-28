---
id: 20260828-d6a3f2
title: LVM Crash-Prefix Conformance
status: completed
created: 2026-08-28
updated: 2026-08-28
branch: wip/lvm-crash-prefix-conformance
pr:
supersedes: []
superseded_by:
---

# LVM Crash-Prefix Conformance

## Summary

- Added a root-only Ubuntu 24.04 conformance harness for one deliberately
  narrow recovery boundary: a stopped writer, an fsynced plain-JSONL rollout,
  an atomic LVM/device-mapper block snapshot, and offline tail repair on a
  detached writable copy. The harness makes no independently verified whole-
  filesystem freeze/flush claim.
- Kept the production Linux ext4 backend capability tuple and public
  checkpoint surfaces unchanged.

## Current State

- The harness writes complete fsynced JSONL records followed by one fsynced
  synthetic partial suffix on an ext4 origin LV, kills and joins that exact
  writer, fsyncs the rollout directory, then creates an atomic mounted-origin
  LVM/device-mapper block snapshot.
- The read-only snapshot is exported and fsynced as a raw artifact. That
  artifact is mode `0400`, attached to a read-only loop, and mounted with
  journal replay disabled. Its size and SHA-256 remain fixed while
  `repairStoppedRolloutTails()` runs only against an independent full
  byte-stream copy mounted read-write.
- The repaired copy retains every complete record, removes only the invalid
  suffix, contains no invented `turn_aborted` event or abort marker, accepts
  one new synced valid event, and passes a full reread.
- The dedicated CI job opts in explicitly, installs `acl`, `lvm2`,
  `e2fsprogs`, `udev`, and `util-linux`, runs the harness as root, and fails if
  a scoped mount, loop device, volume group, or working root remains after the
  test.

## Safety Boundary

- The harness does not independently verify a whole-filesystem freeze/flush.
  Its durable evidence is limited to the rollout file and directory entry
  explicitly fsynced before the block snapshot. This does not simulate sudden
  host power loss, storage-controller cache loss, or a device that violates
  acknowledged-write durability.
- Stopping and joining the exact writer is required. The harness neither
  fences a partitioned stale writer nor proves automatic failover.
- This is conformance evidence, not a production adapter. It does not emit or
  publish a `crash-prefix` checkpoint descriptor, alter
  `atomicPointInTimeCheckpoint: false`, add a public capture/restore method, or
  compose PostgreSQL catalogue and launcher admission.

## Follow-up

- Keep sudden-power-loss/controller-cache-loss evidence, a production
  crash-prefix checkpoint adapter, and automatic stale-writer fencing as
  separate work.
- Keep Git Summary deferred; it is not checkpoint or recovery authority.

## Validation

- `node --check` passed for the conformance harness.
- The non-privileged harness test passed; the explicit LVM case skipped without
  its opt-in environment.
- The controlled full Node test suite passed with installed-Codex live probes
  skipped.
- Workflow YAML parsing, shell-block `bash -n`, project-journal validation, and
  `git diff --check` passed. `shellcheck`, `actionlint`, and `yamllint` were not
  available locally.
- The privileged LVM/device-mapper path is owned by the dedicated Ubuntu 24.04
  CI job because the local macOS host has no Linux LVM runtime.

## Evidence

- Harness: `integration/linux-ext4-crash-prefix-conformance.mjs`
- CI gate: `.github/workflows/test.yml`
- Recovery primitive: `src/rollout-tail-repair.mjs`
- Architecture boundary: `docs/architecture/linux-ext4-physical-backend.md`
