---
id: 20260723-b7419e
title: Session Authority Foundation
status: completed
created: 2026-07-23
updated: 2026-07-23
branch: wip/production-runtime-authority
pr:
supersedes: []
superseded_by:
---

# Session Authority Foundation

## Summary

- Added a same-client PostgreSQL `SERIALIZABLE READ WRITE` executor with
  database transaction time, bounded transaction-rollback retry, explicit
  commit-state classification, per-query transaction-boundary checks,
  an explicit dedicated-pool API and session reset, and a callback-scoped
  extended-protocol query capability.
- Added the checksum-bound initial authority schema for canonical sessions,
  operation and reservation claims, capture-attempt claims and tombstones, and
  the checkpoint catalogue.
- Added a bounded runnable-image profile for exact OCI/Docker
  platform-manifest, config, layer/rootfs, and Codex executable measurement
  with a one-use process-local image reservation.

## Current State

- A PostgreSQL transaction uses one checked-out client from `BEGIN` through
  `COMMIT` or `ROLLBACK`. Only driver-originated `40001` and `40P01`
  transaction rollbacks are retried; arbitrary callback errors cannot request
  replay. The dedicated pool is reset with verified `DISCARD ALL` before and
  after each proved transaction boundary. A user-query failure without a
  trusted PostgreSQL SQLSTATE destroys the client and remains
  outcome-uncertain. SQLSTATE provenance, pending-query tracking, and
  post-callback boundary checks use module-captured intrinsics, own driver
  result fields, and a captured `DatabaseError` prototype check. Built-in
  prototype poisoning by a callback limited to the transaction capability
  cannot convert a local or unknown outcome into a retry. The dedicated pool,
  client, connection event source, and node-postgres implementation remain an
  explicit trusted boundary and must not be exposed to callbacks.
- Store errors thrown through the callback are trusted only when minted by the
  exact current transaction attempt. Module-private constructor identity
  covers alternate `newTarget` construction, while the captured public
  prototype chain covers prototype-created counterfeits. Publicly constructed,
  cross-operation, prototype-forged, and opaque Proxy errors become a definite
  generic rollback result, so forged or stale `commitState` cannot contradict
  the current transaction outcome.
- Migration validation errors retain their specific code only when marked by
  the current `migrate()` invocation. Publicly constructed errors, internal
  errors replayed from another operation, and inherited prototype accessors
  cannot forge a committed migration outcome after the current transaction
  has rolled back.
- Database `transaction_timestamp()` supplies one canonical clock value to the
  complete callback. Query capabilities expire after the callback and an
  unobserved or suppressed failed query cannot be committed as success. Local
  validation rejections are internally observed without changing the rejected
  promise returned to the caller, so fire-and-forget misuse cannot become an
  unhandled process rejection. `PREPARE TRANSACTION` is rejected before
  submission, including leading empty statements and PostgreSQL
  comment-separated spellings, so a callback cannot strand the transaction and
  its locks outside the executor boundary; ordinary server-side prepared
  statements remain available and are removed by the post-transaction reset.
- Generic commit transport failures remain outcome-uncertain. A server-returned
  transaction-rollback SQLSTATE is definitely not committed and may retry
  within the configured bound.
- Migration application is serialized by a PostgreSQL advisory transaction
  lock and binds version 1 to the exact LF-normalized tracked SQL SHA-256.
  Unknown, additional, or checksum-mismatched installed versions fail closed.
- A trusted image inspector must return the exact normalized Codex version,
  binary path, and SHA-256 derived from the descriptor/config identity. The
  reservation is an opaque same-process object capability and is terminal on
  concurrent use, evidence drift, inspector replacement, or uncertainty.
  Manifest and config byte views are rejected by intrinsic length before any
  source-sized private byte-buffer allocation and copied without invoking
  shadowable source properties. Their JSON is also rejected before full parse
  when bounded node, member, element, container, layer, DiffID, or history
  budgets are exceeded. Inspector-returned, authority-internal, and public
  operation Promises receive a captured own constructor before await or
  return, so inspector mutation of Promise prototype accessors cannot forge a
  measurement, revalidation, or consumption result.
- Real PostgreSQL CI applies the migration, creates a genuine concurrent
  serializable conflict, verifies bounded whole-callback retry, and exercises
  the active partial-unique indexes.
- The foundation does not yet implement session lifecycle transitions,
  authorize a lease or launch, verify registry publisher trust, pin a runtime
  mount, or provide a physical stale-writer fence.

## Next Steps

- Implement canonical session registration and readback.
- Add database-clock lease acquisition and renewal with uint64 fencing epochs.
- Add reserved attachment, release, force-fence, catalogue, and launcher
  transitions with exact revision finalization and explicit reconciliation.
- Keep the manual-fencing stopped-directory backend ineligible for automatic
  takeover.

## Evidence

- `src/postgres-serializable-store.mjs`
- `migrations/authority/001-session-authority.sql`
- `src/platform-image-reservation.mjs`
- `test/postgres-serializable-store.test.mjs`
- `integration/postgres-session-authority.mjs`
- `test/platform-image-reservation.test.mjs`
- `docs/architecture/session-runtime-authority.md`
