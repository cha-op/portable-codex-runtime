---
id: 20260810-b91f4a
title: Restore Generation Publication Binding
status: completed
created: 2026-08-10
updated: 2026-08-10
branch: wip/restore-generation-publication-binding
pr:
supersedes: []
superseded_by:
---

# Restore Generation Publication Binding

## Summary

The typed PostgreSQL restore-generation authority already issues one complete
binding between the source capture, old attachment, destination generation,
restore request, and durable operation reservation. The stopped-directory
backend previously rebuilt only a historical four-field projection before
publication, so a fresh callback could not produce the full coordinator-binding
digest required by the typed generation document.

Backend contract version 3 adds an explicit full-binding callback without
changing the legacy version 2 path. It can route that binding either to fresh or
exact-replay publication, or to source-free committed verification. Production
`runRestore()` remains fail-closed; this workstream closes only the publication
transport seam required by the later adapter composition.

## Protected Property

The publication journal must hash and retain the exact authority-issued
generation binding. The backend may defensively canonicalize that binding, but
it cannot replace it with a smaller projection, infer missing provenance, or
mix fields from another checkpoint, restore request, attachment, reservation,
or destination.

Committed-only recovery must remain read-only. It may verify one exact committed
destination without receiving a mutable source path, and it must never prepare,
copy, rename, commit, or advance an earlier publication phase.

## Scope

- Raise `STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION` to `3`.
- Preserve the omitted or explicit version 2 restore callback and its historical
  four-field binding plus `publishRestoreDestination()` behavior.
- Add an exact version 3 callback carrying the complete eleven-field generation
  binding and a closed publication mode.
- Route `fresh-or-exact-replay` only to `publishRestoreDestination()` and
  `committed-only` only to `verifyCommittedRestoreDestination()`.
- Require a version 3 materialisation with the exact coordinator-binding digest
  on both production routes, and require `replayed: true` from committed-only
  verification. Historical v2 materialisations remain legacy-context replay
  inputs only.
- Rebuild a deeply frozen binding and reject Proxy, accessor, extra-key, and
  cross-field drift before physical publication or verification.
- Keep the frozen completion and public restore result shapes unchanged.

## Binding Decisions

- `checkpoint` and `request` must match the admitted restore and predetermined
  result exactly.
- `captureOperationId` must match the trusted checkpoint artefact proof.
- Destination isolation and operation reservation IDs must match the callback
  context exactly.
- The old attachment must match the restore session, backend, destination
  storage, canonical lease, holder, and fencing epoch.
- The old attachment root is a source path, not the new restore destination. It
  must be canonical and lexically disjoint from both artefact and destination
  owned roots; this path comparison does not prove filesystem-object identity.
- Capture-attempt, catalogue, generation, and attachment proof identifiers have
  no independent callback authority source. They are strictly validated and
  retained within the exact whole binding rather than reconstructed.
- A source checkpoint storage ID may differ from the destination request storage
  ID and is not rewritten to match it.

## Compatibility

An authority that omits `restoreContextContractVersion`, or advertises version
`2`, continues to provide the exact historical twelve-field callback context.
The backend derives the same four-field coordinator binding and may publish a
fresh destination or replay an exact prepared, materialized, or committed
journal state. Version 3 cannot downgrade to that shape: it requires both the
full generation binding and an explicit valid publication mode.

## Acceptance Criteria

- Full-binding fresh publication receives a deeply frozen defensive copy and
  creates a version 3 restore materialization carrying its coordinator digest.
- Committed-only recovery invokes no publication method and accepts only an
  exact committed destination with `replayed: true`.
- Every provable binding mismatch fails before the publication collaborator is
  called; raw nested mutations after callback admission do not alter the
  canonical binding.
- Source attachment roots equal to, above, or below either the artefact owned
  root or the destination owned root fail closed.
- Invalid callback-version negotiation fails during backend construction with
  zero resolver, lifecycle, authority, or publication collaboration.
- Legacy version 2 fresh and exact replay behavior remains unchanged.

## Validation

- `node --check` passes for the changed source and test modules.
- The complete `test/stopped-directory-backend.test.mjs` file passes with 146
  tests, including legacy, version 3 fresh, committed-only, path-isolation,
  hostile-shape, and constructor-negotiation coverage.
- The complete repository unit suite passes when skipping only the known
  host-limited `chatgptAuthTokens refreshes after 401 without writing
  auth.json` case. The unfiltered suite reaches that single failure with
  `EMFILE: too many open files, watch`; all other tests pass.
- Project-journal validation and `git diff --check` pass.
- Real-PostgreSQL integration is left to CI because
  `SESSION_AUTHORITY_DATABASE_URL` is absent locally; this slice changes no
  PostgreSQL query or schema.

## Remaining Serial Work

1. Admit capture-bound activation through the exact
   detach-to-generation-to-capture-to-stop predecessor chain and teach the real
   launcher to prepare and consume activation-materialized launch attempts.
2. Add a cross-process shared/exclusive restore lifecycle guard and bounded
   production recovery scheduler so recovery cannot cancel a foreground
   prepared launch.
3. Compose stop, capture, generation publication, detach, activation, prepared
   launch, fleet admission, and durable recovery through production
   `runRestore()`.

## Non-Goals

- No production restore enablement or detached-production fleet gate.
- No activation, launcher, recovery-runner, cursor, or database-schema change.
- No physical filesystem-image, block-volume, NFS, Podman, or Docker backend.
- No provider reconciliation extension, periodic snapshot scheduler,
  differential compression, retention, encryption, cross-host transport, auth,
  or Git Summary.
