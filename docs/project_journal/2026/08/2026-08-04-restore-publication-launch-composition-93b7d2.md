---
id: 20260804-93b7d2
title: Restore Publication-to-Launch Composition
status: superseded
created: 2026-08-04
updated: 2026-08-05
branch: wip/restore-publication-launch-composition
pr: https://github.com/cha-op/portable-codex-runtime/pull/27
supersedes: []
superseded_by: 20260805-4c7a91
---

# Restore Publication-to-Launch Composition

## Summary

> [!IMPORTANT]
> This workstream was reverted after review proved that it equated an existing
> active attachment root with the absent final pathname required by restore
> publication. See the superseding attachment-contract correction journal.
> The remaining present-tense prose is preserved as a historical record of the
> PR #27 tree at merge commit `65559ea`; it does not describe the current tree.

Added a production-neutral composition facade (a standalone implementation not
wired into the production checkpoint adapter) for the ordered restore path:
typed durable read, fresh-only fleet gate, version 2 claim, exact physical
publication, independent committed verification, atomic handoff, then prepared
launch. Restore callback contract version 3 now carries the full typed
generation binding unchanged through the stopped-directory backend and
publication journal, while production `runRestore()` remains intentionally
unavailable.

## Historical State at `65559ea`

- `PostgresSessionAuthority` accepts
  `restoreLaunchV2FleetCompatible: true` only as an explicit startup decision.
  The default is false. The check occurs only after exact operation replay
  lookup and therefore blocks fresh version 2 reservation without blocking
  existing replay, finalisation, or recovery.
- `createPostgresRestorePublicationLaunchComposition()` first performs an
  exact typed durable read. Only an absent operation requires its
  invocation-time `fleetCapabilityGate()` to return the frozen
  `RESTORE_LAUNCH_V2_FLEET_CONFIRMED` sentinel before restore preparation or
  durable creation; exact replay bypasses that fresh-work gate. A terminal
  version 2 `cancelled-before-dispatch` read preserves its committed revision-1
  operation and released reservation but has no generation or catalogue; the
  facade rejects it immediately before gating, preparation, or publication.
- The facade prepares the isolated destination, opaque image reservation, and
  durable launch intent before taking the restore operation guard. Under the
  guard it re-reads the exact request, reserves or claims only when required,
  calls the backend publisher once, independently verifies the committed
  publication through the bound real publication instance, and confirms the
  atomic handoff. Keeping preparation outside the guard avoids shared-pool
  starvation while retaining the publication-sensitive serialization
  boundary.
- A committed restore replay treats the canonical durable request as the
  launch-intent authority. It revalidates the complete operation request
  digest, reuses the persisted launch intent, and does not revalidate an image
  capability that the first successful launch already consumed. A launch
  attempt that is still prepared remains responsible for validating and
  consuming its original opaque reservation before dispatch.
- Physical publication receives the authority-returned full generation
  binding plus the exact artefact, lease, storage, destination, path, and
  predetermined-result context. Its coordinator-binding digest must match the
  same generation binding before handoff. Before launch preparation or any
  durable reserve/claim, the composition requires the prepared canonical
  destination to equal the expected session attachment root; the authority
  receipt later binds the claim attachment exactly to that expected
  attachment. The stopped-directory backend independently enforces the same
  canonical root equality for every version 3 journal binding before
  publication.
- Restore callback contract version 3 requires and validates that full
  generation binding plus an explicit publication mode, then passes the same
  frozen object unchanged to the publication journal. A legacy version 2
  callback has neither field; the backend derives its historical four-field
  version 1 journal binding internally and permits only committed read-only
  verification. It cannot authorize a fresh publication, resume a
  non-committed legacy publication, or replace typed generation identity.
- Handoff acknowledgement loss replays only the same handoff under the held
  guard, after a fresh ownership assertion. The complete handoff receipt is
  bound back to both durable operations and reservations, the generation and
  catalogue, the exact launch request, and the canonical session relation.
  After handoff confirmation, the guard is released and
  `runPreparedLaunch()` receives the original opaque reservation through the
  same frozen shallow wrapper snapshot used to prepare the launch, plus the
  exact pre-reserved launch-attempt ID; it does not reserve another operation.
  Its `started` result is accepted only when the full terminal
  operation/reservation/session/evidence relation validates and the durable
  revision is exactly 2 after prepared/starting, exactly 3 after uncertain, or
  unchanged for committed replay. Stable session identity—including its
  manifest, storage, backend capabilities, attachment, lifecycle, recovery
  state, writer epoch, document version, creation time, session ID, and
  committed launch pointer—must still match the atomic handoff. A later
  single committed authority-validated operation may advance operation
  pointers, revision and update time, and a canonical lease-renew result may
  extend the same lease identity. The read receipt carries that operation's
  complete predecessor, result and released-reservation relation, and the
  launcher retains its same-process authority-read identity. Active,
  multi-step, detached or self-constructed proofs, same-revision content drift,
  lease replacement or expiry rollback, and access-policy changes remain
  rejected.
- A supported version 2 expected session remains unchanged inside the durable
  operation request while the authority upgrades its current active and
  terminal session receipts to document version 3. Those receipts must match
  the complete expected document after applying only that version upgrade and
  the authority-owned active-operation pointer. Version 1 restore sessions
  remain unsupported and fail before fleet gating or durable work.
- Committed-only restore verification classifies physical publication only
  after binding the target filesystem/root identity and probing candidate and
  final paths under the publication lock. A retained `materialized` candidate
  with an absent final path is proven `not-committed`; a final-only state after
  rename remains `uncertain`. The locked verifier may first construct an
  internal `not-committed` error, but the outer publication boundary preserves
  `publicationMayHaveOccurred` and reclassifies it as `uncertain` whenever the
  final path was visible or absence was not proved. Verification performs no
  journal transition or path mutation.
- A failure before definite publication dispatch may cancel only the prepared
  restore. A failure after dispatch but before confirmed handoff leaves or
  marks durable uncertainty. A launch failure after handoff remains owned by
  the existing no-relaunch reconciliation path.
- `createPostgresCheckpointMutationAuthority().runRestore()` is unchanged and
  still fails with `postgres_checkpoint_restore_unavailable` without invoking
  its publication callback.

## Historical Safety Decisions at `65559ea`

- The protected property is an unbroken one-to-one chain from the fresh
  version 2 restore operation, through its authority-owned generation binding
  and physical publication evidence, to its permanently registered launch
  attempt. A path, journal row, generation row, serialized image measurement,
  or launch ID alone is insufficient authority.
- Startup fleet compatibility controls only new durable request creation.
  Replay and recovery must stay available even if rollout policy later closes
  the gate; otherwise disabling admission would strand already-authorized
  work.
- The typed durable read runs before the invocation-time fleet gate. The gate
  runs only for absent work and before preparation or database writes, while
  the authority option remains the final fresh-reservation enforcement
  boundary. Neither gate is inferred from a request field or serialized
  capability claim.
- Preparation remains outside both PostgreSQL transactions and the advisory
  guard. Publication and its independent committed-state verification remain
  outside transactions but inside the exact advisory guard. The atomic handoff
  is confirmed before the image reservation can be consumed or the external
  launcher invoked.
- The full generation binding is passed by object identity, not reconstructed
  from a reduced journal projection. Legacy version 1 compatibility is
  read-only and committed-only, so it cannot fabricate the coordinator digest
  required by a fresh typed version 2 generation.
- Publication destination identity is the exact canonical path shared by the
  prepared destination, expected session attachment, authority-validated claim
  attachment, and backend journal binding. Both endpoints pass canonical
  absolute-path validation before equality is checked; unrelated filesystem
  timestamps, link counts, and inode metadata are not identity evidence for
  this property.
- The mutable preparation result is never retained as the image capability
  wrapper. Its four own data fields are sampled once into a frozen wrapper,
  preserving the opaque inner reservation identity while preventing a later
  outer-property swap between launch preparation and execution.
- A one-use image capability protects launch dispatch, not durable replay of a
  launch that is already committed. The canonical operation request digest
  protects the persisted launch intent against substitution, while the writer
  launcher's prepared path remains the sole consumer of the opaque capability.
- Session comparisons protect stable identity, content and access policy, not
  JavaScript object identity or an obsolete whole-snapshot equality. A legal
  version 2-to-3 authority upgrade is accepted only at the current-session
  boundary; every unrelated stable field is reconstructed from the expected
  session and compared as one canonical document. The terminal launcher result
  independently binds that stable identity and committed launch pointer back to
  the handoff receipt. A different snapshot is accepted only through one
  complete DB-validated committed transition whose exact predecessor is the
  handoff snapshot; a module-private weak identity prevents a copied or
  self-constructed relation from substituting for the authority read whenever
  that transition is needed to cross between different snapshots. Exact-equal
  handoff and result snapshots do not depend on that auxiliary identity.
- Journal phase is not physical publication proof. For a committed-only restore
  check, only locked candidate/final topology under the bound storage identity
  can downgrade a `materialized` outcome to `not-committed`; visible final
  state stays uncertain until the durable journal is committed.
- The standalone facade does not provide durable complete-stop proof, capture
  admission, bounded operational recovery, a concrete container driver, or
  production adapter enablement.

## Historical Next Steps at `65559ea`

1. Route exact coordinator stop confirmation through
   `writer-launch-stop-v1`, retain only same-process capability state that can
   still prove writer identity, and join durable complete-stop proof to later
   capture admission.
2. Compose bounded generation, prepared-launch, active-attempt, and
   current-launch recovery without relaunching or reconstructing opaque image
   or writer capabilities.
3. Wire the complete fail-closed protocol into the production checkpoint
   authority's `runRestore()` only after stop/capture and recovery behavior is
   verified across acknowledgement loss and restart.
4. Implement the later filesystem-image backend, differential export,
   retention, and cross-host recovery verification.

## Historical Non-Goals at `65559ea`

- No production `runRestore()` enablement.
- No durable stop-to-capture composition.
- No bounded operational restore or launch recovery service.
- No concrete Podman or Docker launcher.
- No filesystem-image backend.
- No Git Summary implementation.

## Historical Evidence at `65559ea`

- `src/postgres-restore-publication-launch-composition.mjs`
- `src/postgres-session-authority.mjs`
- `src/stopped-directory-backend.mjs`
- `src/postgres-checkpoint-mutation-authority.mjs`
- `test/postgres-restore-publication-launch-composition.test.mjs`
- `test/postgres-session-operation-kernel.test.mjs`
- `test/stopped-directory-backend.test.mjs`
- `test/postgres-checkpoint-mutation-authority.test.mjs`
- `integration/postgres-session-authority.mjs`
- `createPostgresRestorePublicationLaunchComposition()`
- `RESTORE_LAUNCH_V2_FLEET_CONFIRMED`
