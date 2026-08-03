---
id: 20260803-c0a7f4
title: Restore Generation Schema Foundation
status: completed
created: 2026-08-03
updated: 2026-08-03
branch: wip/restore-generation-schema
pr:
supersedes: []
superseded_by:
---

# Restore Generation Schema Foundation

## Summary

Added the ordered migration and relational schema foundation needed to retain
canonical restore destination generations without enabling restore or launcher
admission.

## Current State

- `PostgresSerializableStore.migrate()` loads the fixed migration set before
  database access and treats the installed ledger as an exact contiguous
  prefix starting at version 1.
- Every installed migration checksum must match its immutable tracked SQL
  source. Gaps, future versions, malformed rows, and checksum drift fail closed.
- One advisory-locked transaction applies every missing migration in order and
  records each version and checksum before the commit boundary.
- Migration version 2 adds the permanent
  `session_authority.restore_destination_generations` relation.
- Generation identity, operation identity, session identity, checkpoint
  identity, session revision, and future restore fencing remain separate
  concepts.
- Composite foreign keys bind each generation to the same session's permanent
  operation claim and checkpoint catalogue entry.
- `authorized` rows cannot contain a finalized document or commit timestamp;
  `committed` rows require both.
- The authority exposes no generation delete or retirement API in this slice.

## Safety Decisions

- Version 1 SQL remains immutable. Version 2 is a separate checksum-bound
  migration.
- The migration ledger is not best-effort metadata. Any non-prefix state or
  mismatched checksum blocks migration before further DDL is applied.
- The generation relation protects permanent identity and relational linkage,
  not publication, destination isolation, object identity, content stability,
  physical fencing, or launch authorization.
- A generation table row, checkpoint catalogue row, published restore path, or
  journal result is not writable-launch authority.
- The production checkpoint adapter remains capture-only and restore remains
  fail-closed.

## Remaining Work

1. Add typed restore-generation claim, dispatch, finalization, exact replay,
   and recovery transitions.
2. Add the durable launch-attempt lifecycle without invoking a launcher inside
   the database-authority slice.
3. Compose finalized generation admission, measured-image capability
   consumption, external launch, supervisor evidence, and exact writer
   registration before enabling production restore.
4. Implement the later physical filesystem-image backend and cross-host
   checkpoint layers.

## Non-Goals

- No canonical session document change.
- No restore publication callback.
- No typed restore-generation mutation API.
- No durable launch-attempt transition.
- No Podman or Docker launch.
- No Git Summary implementation.

## Evidence

- `src/postgres-serializable-store.mjs`
- `migrations/authority/001-session-authority.sql`
- `migrations/authority/002-restore-destination-generations.sql`
- `test/postgres-serializable-store.test.mjs`
- `integration/postgres-session-authority.mjs`
- `README.md`
- `docs/architecture/runtime-delivery-plan.md`
- `docs/architecture/session-runtime-authority.md`
