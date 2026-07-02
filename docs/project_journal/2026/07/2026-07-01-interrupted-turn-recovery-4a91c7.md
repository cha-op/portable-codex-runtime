---
id: 20260701-4a91c7
title: Interrupted-Turn Recovery Spike
status: completed
created: 2026-07-01
updated: 2026-07-01
branch: wip/interrupted-turn-recovery
pr: 3
supersedes: []
superseded_by:
---

# Interrupted-Turn Recovery Spike

## Summary

- Characterized explicit interruption, `SIGTERM`, `SIGKILL`, and stopped-tree
  restore behavior through a real Codex app-server and loopback model mock.
- Preserved a deterministic probe, safety tests, source analysis, and redacted
  evidence with no credential input provisioned and the model provider
  configured for a loopback mock.

## Current State

- Explicit `turn/interrupt` persists the model-visible abort marker and reports
  a terminal interrupted turn.
- Signal and hard-kill recovery normalize the incomplete turn to interrupted in
  resume/read views but do not synthesize the missing marker.
- The same explicit thread ID resumes after the complete session tree is copied
  only after process exit, removed, and restored at a different absolute path.
- The copy preserves portable symlinks without following them and fails closed
  on inaccessible entries, symlink chains that leave the source tree,
  non-relocatable links, special permission bits, hard links, sockets, FIFOs,
  and devices.
- The copy holds a current-user-owned, mode `0700`, extended-ACL-free root and
  validates its complete trusted ancestor chain;
  redacted evidence publication separately requires a pre-existing trusted
  directory authority, retains pre-rename temp artifacts for owner cleanup, and
  reports post-rename directory-sync failures through the CLI as
  `evidence_durability_uncertain` without exception details.
- Codex flush is not an fsync barrier, rollout-tail repair remains absent, and
  background terminals are not filesystem-migratable state.

## Next Steps

- Define session filesystem, manifest, lease, fencing, and pluggable storage
  contracts in the next serial pull request.
- Keep stopped-tree compatibility evidence separate from the later production
  snapshot, differential compression, retention, and restore implementation.

## Evidence

- `docs/experiments/interrupted-turn-recovery.md`
- `evidence/interrupted-turn-recovery.json`
- `CODEX_BIN=/opt/homebrew/bin/codex npm test`
- `CODEX_BIN=/opt/homebrew/bin/codex npm run probe:turn-recovery -- --write-evidence`
