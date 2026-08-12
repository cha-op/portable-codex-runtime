---
id: 20260812-b6d4e1
title: Restore Activation Reconciliation
status: completed
created: 2026-08-12
updated: 2026-08-12
branch: wip/activation-reconciliation
pr:
supersedes: [20260805-3f91c2]
superseded_by:
---

# Restore Activation Reconciliation

## Summary

- Preserved the authority's one-shot activation dispatch grant through the
  live foreground claim without adding it to durable operation state.
- Added read-only provider reconciliation so retained or ambiguous activation
  never repeats a physical attach.

## Current State

- Durable restore-attachment request and result contract version 1, operation
  hashes, terminal evidence, and the PostgreSQL schema remain unchanged.
- The storage backend has a separate reconciliation contract version 1. It
  observes the activation request's stable mutation operation ID and returns
  exactly `applied`, `absent-and-quiescent`, or `unknown`.
- The coordinator reconciles before physical attachment. `applied` carries an
  exact activation result that can be finalized without dispatch.
  `absent-and-quiescent` permits the first `prepareRestoreAttachment()` only
  when the current foreground invocation still holds the definite one-shot
  claim grant. `unknown` fails closed.
- Retained `starting` is marked uncertain before read-only recovery. Retained
  `uncertain`, claim replay, claim acknowledgement loss, and process restart do
  not reconstruct the grant and therefore cannot issue a second attach.
- The stopped-directory adapter delegates preparation and reconciliation only
  when the lifecycle backend supplies both complete versioned extensions.

## Non-Goals

- Reconciliation is trusted physical observation; it is not cancellation,
  fencing, quiescence derived from a database row, or permission to retry an
  ambiguous attach.
- This slice does not add physical-collaborator deadlines to activation,
  supervisor, storage lifecycle, or publication calls. It selects no lease
  budget and does not enable the public restore route.
- No schema, migration, durable operation shape, physical backend, container
  runtime, or recovery lane is added.

## Next Steps

1. Apply method-specific settlement to the full mutating physical graph,
   preserving this activation reconciliation rule.
2. Admit the operational lease budget only after every critical method has a
   bounded settlement policy.
3. Run the assembled restart, acknowledgement-loss, deadline, grace-breach,
   and ambiguous-outcome matrix before enabling the final public backend.

## Evidence

- `src/session-storage-contracts.mjs`
- `src/stopped-directory-backend.mjs`
- `src/postgres-detached-restore-foreground-composition.mjs`
- `src/postgres-restore-activation-recovery-coordinator.mjs`
- `test/session-storage-contracts.test.mjs`
- `test/stopped-directory-backend.test.mjs`
- `test/postgres-detached-restore-foreground-composition.test.mjs`
- `test/postgres-restore-activation-recovery-coordinator.test.mjs`
