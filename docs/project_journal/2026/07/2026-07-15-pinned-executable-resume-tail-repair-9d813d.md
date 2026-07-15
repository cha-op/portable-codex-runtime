---
id: 20260715-9d813d
title: Pinned-Executable Resume and Rollout-Tail Repair
status: completed
created: 2026-07-15
updated: 2026-07-15
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
  permissions, aliases detectable by the adapter, and identity races.
- The schema-v6 live probe exercises both modifying actions with one staged
  private Codex executable on stopped-tree-derived writable copies, resumes by
  explicit thread ID and restored `cwd`, completes one follow-up, and verifies
  it through a third fresh cold read. It does not exercise a production backend
  restore.
- The result proves `same-pinned-executable` compatibility. Production OCI
  same-image resolution and admission remain pending.

## Validation

- Targeted `node --test` over the repair and recovery-probe files: 137 tests,
  134 passed and 3 platform/explicit-live conditions skipped.
- Pinned `npm run probe:turn-recovery -- --write-evidence`: all six live
  app-server scenarios passed and schema-v6 redacted evidence was published.
- Pinned `npm test`: 1,097 tests, 1,095 passed and 2 platform conditions
  skipped.
- Project-journal validation and `git diff --check` passed.

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
