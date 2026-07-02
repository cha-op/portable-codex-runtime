---
id: 20260702-7c31e2
title: Session Filesystem and Storage Contracts
status: completed
created: 2026-07-02
updated: 2026-07-02
branch: wip/session-storage-contracts
pr:
supersedes: []
superseded_by:
---

# Session Filesystem and Storage Contracts

## Summary

- Defined executable v1 record validators for immutable session identity,
  storage references, host-local directory attachments, lease/fencing values,
  structural rootless worker binds, declared backend capabilities, and
  checkpoint classes.
- Kept auth authority, canonical lease state, host paths, and Git Summary out of
  the portable session manifest and checkpoint authority.

## Current State

- A rootless worker sees a normal directory at `/session`; host storage agents
  retain raw-device, image, attach, mount, and fencing responsibility.
- The manifest records the app-server-returned root thread ID and shared Codex
  session-tree ID, trusted-resolved platform OCI descriptor, persistent history
  settings, stable layout, external auth mode, and agent limits.
- Canonical uint64 fencing epochs live in a linearizable control plane. Lease
  expiry alone is not physical fencing, and uncertain attachment state blocks
  automatic migration.
- Checkpoint classes distinguish `clean`, `graceful-abort`, and `crash-prefix`
  recovery. Their descriptors are observations, not stop or fence proofs;
  concrete restore adapters must acquire a newer canonical writer epoch and
  retain physical fence authority through launch.

## Next Steps

- Implement the auth broker against the external-token and authority contracts.
- Implement concrete storage attachment and checkpoint adapters in their later
  serial pull requests.

## Evidence

- `docs/architecture/session-storage-contracts.md`
- `src/session-storage-contracts.mjs`
- `test/session-storage-contracts.test.mjs`
- Codex source commit `db887d03e1f907467e33271572dffb73bceecd6b`
