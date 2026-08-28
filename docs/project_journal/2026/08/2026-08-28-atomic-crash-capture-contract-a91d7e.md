---
id: 20260828-a91d7e
title: Atomic Crash-Capture Contract
status: completed
created: 2026-08-28
updated: 2026-08-28
branch: wip/atomic-crash-capture-contract
pr:
supersedes: []
superseded_by:
---

# Atomic Crash-Capture Contract

## Summary

- Added a dormant provider-neutral version 1 extension for one physical
  `crash-prefix` capture without changing the base storage backend contract,
  the clean checkpoint path, or any public deployment surface.
- Added a separate process-local core that freezes one normalized request
  behind an object-identity token, consumes that token before provider
  dispatch, and preserves acknowledgement loss as an uncertain outcome.
- Added source-free committed-result verification with only `committed` and
  `unknown`; `unknown` is never evidence of absence or permission to dispatch
  again.

## Current State

- The exact request binds one capture-attempt ID, predetermined crash-prefix
  descriptor, checkpoint mutation, source attachment, and storage reference.
  Backend, session, storage, holder, lease, source epoch, operation, checkpoint,
  and artifact identities must all agree.
- A backend opts into the dormant extension through exact data properties for
  `atomicCrashCaptureContractVersion`, `captureAtomicCrashCheckpoint()`, and
  `verifyCommittedAtomicCrashCheckpoint()`. The narrow null-prototype facade
  captures those methods once, fixes their receiver, and requires the backend
  to declare `atomicPointInTimeCheckpoint: true`.
- One prepared token authorizes at most one same-process invocation attempt.
  It is consumed synchronously before calling the provider, so rejection,
  malformed output, or acknowledgement loss cannot reopen that token.
  Independently creating another token remains possible and is not durable
  deduplication; a future authority and provider catalogue must own that rule
  across restarts.
- A committed result echoes the durable capture identity selected from the
  request and includes one provider proof ID plus an artifact observation.
  The original request remains authoritative for its attachment, holder, and
  lease binding. Exact replay must reproduce every result field. Source-free
  verification receives only the normalized request, performs no capture, and
  returns either that exact result or `unknown`.

## Protected Properties

- Object identity is selected by `(objectIdentityScheme, objectId)`. Both
  signals are required because an object ID is meaningful only inside its
  provider namespace.
- Content stability is selected by `(byteLength, contentSha256)`. Length binds
  the observed byte extent, while SHA-256 binds every byte and detects
  same-length mutation. Identical content does not prove identical object
  identity.
- Access policy is selected by `readOnly: true`. Stable identity and bytes do
  not compensate for a writable committed artifact. Unreadable or failed
  policy revalidation remains uncertainty.
- Timestamp, child-entry, mount-bookkeeping, cache, and benign materialization
  changes are not mutations of these selected properties unless the
  corresponding identity, content, or access-policy signal also changes.

## Safety Boundary

- The extension does not authenticate physical writer stop or fencing. The
  opaque authority passed at dispatch must be created and revalidated by a
  future composition and concrete provider.
- It supplies no LVM, device-mapper, ext4, cloud, or other snapshot adapter;
  no durable provider operation journal or catalogue; and no tail repair,
  writable restore generation, or higher-epoch writer admission.
- The current Linux ext4 backend remains manual-fencing and continues to
  advertise `atomicPointInTimeCheckpoint: false`; it does not implement or
  expose this extension.
- The existing clean capture, restore, PostgreSQL authority, deployment facade,
  and public checkpoint backend remain unchanged and continue to reject or omit
  crash-prefix operations.

## Follow-up

- Add a concrete atomic-capture provider and durable exact-result catalogue.
- Bind a complete writer stop or physical fence to the capture admission;
  automatic stale-writer takeover requires the physical-fence branch.
- Restore only to a separate writable generation, prove rollout-tail repair,
  then admit a new writer under a strictly higher canonical fencing epoch.
- Keep host, controller, and drive cache-loss evidence, distribution,
  compression, encryption, retention, registry trust, and remote transport in
  separately scoped work.

## Validation

- `node --check` passed for both storage-contract and crash-capture core source
  files and their focused test files.
- The combined storage-contract and crash-capture core suites passed with 92
  tests.
- The unfiltered repository suite ran 3,481 tests: 3,444 passed, 36 skipped,
  and the sole failure was the unchanged external-auth watcher test after the
  host returned `EMFILE: too many open files, watch`.
- The repository suite then passed with the exact watcher test excluded by
  name: 3,480 tests, 3,444 passed, 36 skipped, and zero failures.
- Project-journal validation and `git diff --check` passed.

## Evidence

- Contract: `src/session-storage-contracts.mjs`
- Process-local core: `src/session-crash-capture-core.mjs`
- Contract tests: `test/session-storage-contracts.test.mjs`
- Core tests: `test/session-crash-capture-core.test.mjs`
- Architecture: `docs/architecture/atomic-crash-capture-extension.md`
