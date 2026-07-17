---
id: 20260715-9d813d
title: Pinned-Executable Resume and Rollout-Tail Repair
status: completed
created: 2026-07-15
updated: 2026-07-17
branch: wip/same-image-resume-tail-repair
pr: 13
supersedes: []
superseded_by:
---

# Pinned-Executable Resume and Rollout-Tail Repair

## Objective

- Verify explicit-thread-ID recovery and subsequent rollout append with one
  pinned Codex runtime identity.
- Add an offline, fail-closed repair primitive for the final JSONL tail before
  a restored Codex home can be reopened for writable resume.

## Delivery Plan

1. Pin the current upstream loader, resume, and append behavior to an exact
   Codex source commit and executable version/digest.
2. Implement stopped-tree rollout discovery and repair without deriving a
   filename from the thread ID or trusting a stale index as authority.
3. Cover clean newline-terminated files, a complete final record without a
   newline, and a torn final record. Reject non-tail corruption, ambiguous
   ownership, unsafe filesystem objects, and uncertain durability.
4. Exercise the supported damage classes through a real app-server using the
   same private executable before and after recovery, then verify a completed
   follow-up append with another cold read.
5. Run the complete repository tests, publish redacted compatibility evidence,
   update architecture docs, and hand the fixed range to the review workflow.

## Scope Boundaries

- The repair runs only against a stopped or detached restored tree before any
  app-server process can reopen the rollout for append.
- The immutable checkpoint remains the original crash evidence. Repair mutates
  only a restored writable copy and returns content-free before/after proofs.
- JavaScript syntax validation is a pinned compatibility adapter, not a stable
  Codex rollout schema. The live probe must confirm the selected runtime reads
  the repaired prefix and appends a separately readable record.
- A trusted OCI resolver and launcher may bind the executable evidence to the
  manifest's exact platform-image digest. This pull request does not implement
  that resolver or treat a host-installed binary as OCI image proof.
- Atomic crash capture, SQLite/WAL repair, background-terminal migration,
  production lease/catalogue/launcher authority, physical image storage,
  differential export, and cross-host restore remain separate workstreams.

## Current State

- `src/rollout-tail-repair.mjs` implements a pinned, offline, fail-closed
  repair pass over the complete discovered session rollout set.
- The pass preserves validated bytes, appends one missing final LF, or removes
  one invalid unterminated tail. It rejects malformed completed records,
  ambiguous session ownership, unsupported formats, unsafe objects,
  permissions, aliases detectable by the adapter, and identity races. Common
  Codex-created `0755`/`0750` directories and `0644`/`0640` rollout files are
  descriptor-pinned, tightened to exact private modes, synced, checked for
  extended ACLs, and revalidated before rollout contents are consumed.
- The schema-v6 live probe exercises both modifying actions with one staged
  private Codex executable on stopped-tree-derived writable copies, resumes by
  explicit thread ID and restored `cwd`, completes one follow-up, and verifies
  it through a third fresh cold read. It does not exercise a production backend
  restore.
- The result proves `same-pinned-executable` compatibility. Production OCI
  same-image resolution and admission remain pending.

## Validation

- Targeted `node --test --test-reporter=dot` over the repair and recovery-probe
  files passed after the review fixes, including permission tightening, ACL
  rejection and race detection, copy-seam isolation, and exact evidence-pin
  checks.
- Pinned `npm run probe:turn-recovery -- --write-evidence` passed all six live
  app-server scenarios under a normal `022` umask. The run used the official
  `rust-v0.144.1` macOS arm64 release asset: archive SHA-256
  `88e72ac8bd30815f7d18e62dac333dc20ce3ad1cba94be1649a1977dd9bfdbb8`
  and extracted binary SHA-256
  `29915529b97697def1a957b0505e770aa6a45744435d62fc263e98d7619e167a`,
  matching the tracked schema-v6 evidence. The host-installed `0.144.5`
  binary was rejected by the compatibility identity gate as designed.
- Pinned `npm test`: 1,097 tests, 1,095 passed and 2 platform conditions
  skipped.
- After migrating credential-shaped test inputs to exact `joey-private-v3`
  catalog entries, the final `npm test -- --test-reporter=dot` run passed
  outside the filesystem sandbox, including its loopback-dependent cases.
- JavaScript syntax checks, project-journal validation, and `git diff --check`
  passed.

## Deferred Work

- Git Summary remains read-only user context and is not recovery authority.
- Review-helper hardening remains outside this repository change: pass only an
  access token through `CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR` instead of
  the temporary full-OAuth-JSON fallback.
- Review-helper DNS classification remains separate: classify `ENOTFOUND` and
  equivalent resolver failures as transient and add regression coverage.

## Evidence

- `docs/experiments/interrupted-turn-recovery.md`
- `evidence/interrupted-turn-recovery.json`
- upstream Codex mirror at
  `db887d03e1f907467e33271572dffb73bceecd6b`
