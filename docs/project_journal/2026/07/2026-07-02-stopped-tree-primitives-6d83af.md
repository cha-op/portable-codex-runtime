---
id: 20260702-6d83af
title: Reusable Stopped-Tree Primitives
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/stopped-tree-primitives
pr: 7
supersedes: []
superseded_by:
---

# Reusable Stopped-Tree Primitives

## Summary

- Extracted the stopped-tree validation, copy, digest, and guarded-cleanup
  closure into a reusable module without changing its filesystem semantics.
- Preserved the interrupted-turn probe's existing public API through imports
  and re-exports.

## Current State

- Callers can reuse the hardened owned-root, ACL, mount-boundary, portable-name,
  symlink, stable-identity, copy, digest, and cleanup rules independently of the
  app-server recovery probe.
- The primitives still require an externally stopped single writer and retain
  a partial destination after a failed copy for trusted-owner inspection or
  cleanup.
- This work does not add an fsync barrier, atomic publication, durable operation
  journal, descriptor replay, destination isolation, or a storage backend.

## Next Steps

- Compose the primitives into a stopped-directory backend conformance slice
  with authentic stopped-writer evidence, atomic fence recheck, a storage
  barrier, destination isolation, and durable idempotent replay.
- Keep same-image resume verification, rollout-tail repair, ext4 or image
  backends, differential export, and Git Summary in their later workstreams.

## Evidence

- `docs/architecture/stopped-tree-primitives.md`
- `src/stopped-tree.mjs`
- `src/interrupted-turn-recovery-probe.mjs`
- `test/interrupted-turn-recovery-probe.test.mjs`
