# Portable Codex Runtime

Portable Codex Runtime is an experimental host runtime for moving Codex
app-server sessions between trusted machines while keeping the execution
environment, workspace, rollout state, and recovery data explicit.

The current repository combines compatibility probes for authentication and
interrupted-turn recovery with an offline, pinned-runtime rollout-tail repair
primitive, the storage contracts, journal, local stopped-directory
publication, same-process stopped-writer authority, and a composed
stopped-directory backend for guarded clean capture, committed-result
reconciliation, and restore.
The planned runtime keeps refresh tokens in a central auth authority, injects
short-lived access tokens into session workers, and treats session data
snapshots separately from monotonic credential state.

## Status

The runtime architecture is under active development. The current implementation
proves that the installed Codex app-server supports external ChatGPT access-token
injection and proves the managed refresh API choreography with an explicitly
uncontained host probe. Production managed refresh fails closed until a
per-refresh rootless containment executor is implemented. A separate loopback
probe characterizes explicit interruption, process signals, hard kills, a
stopped-tree restore, and both supported rollout-tail repairs without using
credentials or a real model turn. The repair compatibility evidence binds one
private Codex executable by version and SHA-256; it does not claim OCI
same-image recovery.

The `chatgptAuthTokens` protocol is an experimental Codex app-server API. Pin the
Codex binary or image digest and rerun these probes before upgrading it.

## Auth Broker MVP

The runtime now includes an encrypted canonical auth-state store and a broker
state machine. AES-256-GCM envelopes bind authority, key ID, monotonic uint64
generation, base generation, commit ID, operation, and payload. Compare-and-
swap publication uses a private directory, advisory lock, same-directory
rename, directory sync, canonical readback, and exact idempotent replay after a
lost acknowledgement.

The broker structurally cross-checks ChatGPT auth mode plus decoded access/ID
JWT identity, plan, and expiration claims; coalesces compatible refreshes;
publishes only an exactly re-read committed generation; and durably blocks
reauthentication or uncertain post-dispatch states. Before OAuth dispatch it
replaces `ready` state with a credential-free durable recovery reservation.
Its unique owner ID prevents ABA dispatch races, while a one-way source-token
digest distinguishes a completed concurrent refresh from a safe restore. A
crash or failed outcome commit therefore cannot expose or reuse the consumed
old refresh token. Canonical realpath identity collapses local filesystem
aliases, and key rotation can advance the envelope generation without
discarding an already-produced outcome. A per-worker facade privately
remembers the last delivered generation and access token, so a stale `401` receives a genuinely
newer credential while a same-token state change does not suppress a required
refresh. Workers receive only the access token, account ID, and plan type
through the pinned experimental app-server protocol.
Ordinary login cannot overwrite an active refresh reservation; explicit crash
recovery requires its exact generation and owner ID after the supervisor has
fenced the old broker process.
See `docs/architecture/auth-broker.md` for the security and recovery contract.

## Session Storage Contracts

The runtime now has executable v1 record validators for a secret-free session
manifest, trusted OCI-resolution matching, uint64 fencing epochs,
lease/attachment matching, declared storage backend capabilities, structural
rootless worker directory binds, and recovery checkpoint classes. Physical
launch, fencing, and snapshot authorization remain the responsibility of later
concrete adapters and their conformance tests.
The worker sees one ordinary directory at `/session`; a host storage agent owns
raw volumes, filesystem images, attach/mount operations, and stale-writer
fencing. `CODEX_HOME`, the effective Codex `sqlite_home`, and the workspace
remain on that single-attached session volume. The launcher fixes
`sqlite_home` through a Codex CLI config override and rejects request-level
changes; auth authority and canonical lease state remain outside the volume.

The default session policy permits 6 subagents, can be raised to a hard limit
of 10, and permits nesting through depth 2. Git Summary remains deferred and is
not part of checkpoint correctness. See
`docs/architecture/session-storage-contracts.md` for the state machine,
backend interface, NFS/image constraints, and Codex source basis.

## Snapshot and Restore Core

The backend-neutral core orchestrates stopped-writer `clean` checkpoint
capture and restore. It validates the manifest, storage, attachment, lease,
and operation request, returns a portable descriptor only after a definite
backend result, requires a restore epoch greater than the source epoch, and
fails closed on every uncertain post-dispatch outcome. Atomic fence rechecks,
storage barriers, durable idempotency, and physical capture or restore remain
backend responsibilities. See `docs/architecture/snapshot-restore-core.md`.

An optional versioned backend extension also lets the core reconcile the exact
original clean-capture request after lease expiry or fence turnover. That path
has no writer, lease, attachment, clock, or stopped-writer capability input: it
can only validate a result already committed by authenticated durable attempt
state and the physical backend.

## Same-Process Stopped-Writer Capability

The runtime can convert one trusted, fully joined writer stop into one
same-process object capability for one snapshot callback. Private object
identity binds the capability to the exact process and writer incarnations,
complete attachment, writer fence, and stop operation. The original reference
may be delegated inside the issuing process, but serialization or
identity-breaking clones produce inert lookalikes and cannot transfer authority
to another host. Stop or snapshot uncertainty is terminal for that writer and
capability, which are never reused to re-dispatch the callback.

Codex lifecycle events such as `turn/completed`, `ShutdownComplete`, and
`thread/closed` are observations rather than writer-stop proof. Production use
requires a supervisor that joins the complete container, cgroup, or VM writer
boundary, or a future Codex shutdown path that joins every persistence writer.
Canonical fence rechecks, durable idempotency, and physical publication remain
backend responsibilities. See
`docs/architecture/stopped-writer-capability.md`.

## Reusable Stopped-Tree Primitives

The stopped-tree validation, copy, digest, and guarded-cleanup logic is now a
reusable module rather than probe-owned code. It preserves the probe's strict
owned-root, mount, pathname, symlink, metadata, and identity-race rules. This
layer still has no fsync barrier, atomic publication, durable operation journal,
descriptor replay, or storage backend. See
`docs/architecture/stopped-tree-primitives.md`.

## Durable Filesystem Operation Journal

The host-local journal durably records exact storage operation state through
`prepared`, `materialized`, and `committed` phases. Canonical copy-on-write
records use file fsync, held-lock rename, parent-directory fsync, and exact
readback; committed results can be replayed after restart. The journal records
caller-supplied state but does not prove physical materialisation, writer stop,
fence authority, atomic publication, destination isolation, NFS guarantees, or
backend success. Its fresh-prepare operation atomically rejects every existing
phase when a higher layer must prove that an operation started inside the
current authority transaction. See
`docs/architecture/filesystem-operation-journal.md`.

## Stopped-Directory Publication

The local stopped-directory publication layer binds that journal to physical
storage work. It holds one publication-root lock, prepares the exact journal
record, establishes a post-order source fsync barrier, builds and fsyncs a
deterministic private stage, records the exact digest and held identity as
`materialized`, atomically renames only onto an absent final destination,
fsyncs and reads back the final object, and only then commits the journal.

Checkpoint artefacts are self-describing `artifact.json` plus `payload/`
bundles. Restore validates the bundle against a trusted capture proof from the
committed catalogue, then publishes only the payload tree. A visible final path
remains unusable by consumers and launchers until its exact journal record is
committed. Partial stages and uncertain final objects are retained as recovery
evidence.

The committed-checkpoint verifier is deliberately narrower than publication.
It accepts no source path and performs no journal transition: it reads and
exactly validates an already committed record and final artefact. `prepared`
and `materialized` operations remain operator evidence and are never advanced
by automatic reconciliation.

This boundary supports only an approved local filesystem. NFS, other remote or
unknown filesystem semantics, canonical fence checks, stopped-writer
authentication, and non-cooperating same-UID races at the final POSIX rename
syscall are outside its guarantee. See
`docs/architecture/stopped-directory-publication.md`.

## Stopped-Directory Backend

The v2 stopped-directory backend composes the same-process capability, a
durable mutation-authority and catalogue seam, and local publication into the
snapshot core's storage-backend contract. It owns only `captureCheckpoint`
and `restoreCheckpoint` in the base contract, and exposes the optional v1
`reconcileCheckpointCapture` extension. Provision, writable attachment
preparation, detach, force-fence, and destroy operations delegate to a
validated lifecycle backend with the same backend ID.

Capture consumes the exact stopped-writer capability once. While the
coordinator callback is active, the mutation authority holds the canonical
fence and admission guard, reserves a predetermined result, runs publication
exactly once, and durably finalizes the catalogue before returning that same
completion. Capture publication must atomically start from an absent journal
operation; it never adopts an earlier prepared, materialized, or committed
artifact as proof of the current stop. Restore applies the same protocol to a
newer fence, trusted artefact proof, and isolated detached destination, while
retaining exact committed replay. Runtime collaborator failures are fixed
path-free uncertainty; the adapter performs no internal retry, speculative
cleanup, or replacement-coordinator recovery.

Inside the one-shot stopped-writer callback, the backend generates a fresh
capture-attempt UUID. Before publication, the authority must atomically claim
that exact UUID and operation in a globally unique durable ledger, create the
authenticated attempt, and retain non-reusable tombstones beyond every
journal, artefact, snapshot, backup, and DR generation that could restore an
old value. Active claim indexes must bind the same canonical attempt record;
retirement atomically changes both to non-authorizing tombstones. Claim
activation, reconciliation, finalization, and retirement share a per-operation
authority transaction or mutex, and finalization revalidates ownership after
asynchronous verification. A separate authority method can later load that
exact actively claimed record and ask the backend to verify only its committed
artefact. It never consumes another stopped-writer capability, reads the old
mutable source, or advances an uncommitted journal phase.

The adapter advertises normal directory attachments, exclusive writers,
`fencing: "manual"`, and
`atomicPointInTimeCheckpoint: false`. It is therefore a trusted local
filesystem development and conformance backend, not an NFS, live-volume, or
automatic failover implementation. The durable authority interface and
conformance tests are part of this slice; a production linearizable database
and catalogue remain separate work. See
`docs/architecture/stopped-directory-backend.md`.

## Interrupted-Turn Recovery

The recovery probe starts a real Codex app-server against a held localhost
Responses API mock. It exercises six independent scenarios:

- stable `turn/interrupt`, followed by a cold resume;
- `SIGTERM` during an active turn;
- `SIGKILL` during an active turn;
- `SIGKILL`, a stopped full-tree copy, deletion of the source tree, and restore
  at a different absolute path;
- `SIGKILL`, removal of the final LF on a stopped-tree-derived writable copy,
  offline `append_lf` repair, resume, one completed follow-up, and a fresh cold
  read;
- `SIGKILL`, injection of an invalid unterminated tail on a stopped-tree-derived
  writable copy, offline `truncate_partial_tail` repair, resume, one completed
  follow-up, and a fresh cold read.

The probe verifies the explicit thread ID through both `thread/resume` and
`thread/read`. Explicit interruption persists a model-visible abort marker.
Signal and hard-kill recovery instead normalizes the stale in-progress turn to
`interrupted` without inventing that marker. The stopped-tree copy preserves
snapshot-user-accessible regular files and directories with their POSIX rwx
permission bits, plus portable UTF-8 symlink targets without following links.
Symlink permission bits are outside the modeled digest. Directory names must be
NFC-normalized. Inaccessible entries, non-ASCII cased names, case-insensitive
name collisions,
dangling relative links, relative-link
case or normalization aliases, traversal through non-directories,
resolution chains that cross protected trees, non-relocatable links, special
permission bits,
hard links (including hard-linked symlinks), sockets, FIFOs, and devices fail
closed. Ownership, ACLs,
extended attributes,
timestamps, and other unmodeled metadata are not preserved or covered by the
digest. If validation or copy fails after destination creation, the partial
destination is retained for cleanup by the trusted owner; the helper never
recursively removes a failure path that another writer could have replaced.
The copy helper requires exclusive single-writer control of its current-user-
owned, mode `0700`, extended-ACL-free root. It holds and revalidates that root,
and requires a trusted owner, permission, identity, and ACL state across the
complete ancestor chain. Concurrent mutation by another process with the same
UID is not a supported security boundary.
It is not an online, atomic, or power-loss-durable snapshot implementation.
The two repair scenarios preserve an immutable backup and use the same staged
private executable before and after repair. This proves
`same-pinned-executable` compatibility only; production OCI same-image
resolution, physical restore, and launcher admission remain separate work.

Run the deterministic compatibility probe with the exact Codex binary from the
pinned runtime image:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery
```

If the system temporary filesystem is mounted `noexec`, set
`CODEX_RECOVERY_EXEC_ROOT` to an existing absolute directory on an executable
filesystem with a trusted owner, ancestor chain, and ACL state; the probe
creates and removes its own mode `0700` subdirectory there.
The pinned macOS runtime must provide `/bin/ls` and `/sbin/mount`; Linux must
provide ACL-capable `/usr/bin/getfacl` plus `/proc/self/mountinfo`. The probe
invokes these fixed inspection surfaces and fails closed when they are absent,
malformed, or when raw Darwin mount paths contain text that is ambiguous with
the `mount(8)` output separators. Both platform tables are captured as bytes
and rejected before parsing if strict UTF-8 decoding would be lossy.

To update the redacted evidence after an intentional runtime upgrade:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery -- --write-evidence
```

The evidence parent directory must already exist, be owned by the current user,
and have trusted permissions, ancestors, and ACL state. Evidence publication
holds and revalidates that directory. A failure before rename retains its
private temp artifact for trusted-owner cleanup; a failure after rename leaves
the destination in place and the CLI reports
`evidence_durability_uncertain` without serializing exception details.

The command provisions no credential input and configures the model provider to
use the loopback mock. It does not impose OS-level outbound network isolation;
run it inside a network-isolated container when that stronger evidence is
required. See `docs/experiments/interrupted-turn-recovery.md` for source
evidence and exact probe semantics, and
`docs/architecture/rollout-tail-repair.md` for the repair contract and storage
limitations.

## Managed Auth Refresh Authority

A central authority can proactively rotate its managed ChatGPT credential
without starting a model turn. The reference adapter runs stable v2
`account/read {refreshToken:true}` against an isolated staging `CODEX_HOME`,
verifies the rotated record, and atomically promotes it into the dedicated
authority home. Concurrent in-process callers share one refresh execution.

The live probe intentionally mutates the dedicated login and then performs a
separate worker turn with the refreshed access token:

```bash
chmod 700 .test-codex-home
CODEX_BIN=/absolute/path/from/the/pinned-image/codex \
  CODEX_ALLOW_AUTH_MUTATION=1 \
  CODEX_ALLOW_UNCONTAINED_AUTH_PROBE=1 npm run probe:auth-refresh:live
```

Do not point this command at the default user `~/.codex` home or the active
`$CODEX_HOME`. The probe rejects path aliases and matching directory identities
and expects `.test-codex-home` or another dedicated authority login.
Production workers must not be able to mount, rename, or write the authority
home or its parent path; only the broker owns that single-attached volume.
See `docs/experiments/auth-refresh-authority.md` for the source evidence,
failure model, and production limitations.

## External Auth Compatibility Probe

The offline probe uses synthetic JWTs, an isolated temporary `CODEX_HOME`, and a
localhost Responses API mock. It verifies that:

- `chatgptAuthTokens` is rejected without `experimentalApi` opt-in.
- The same login succeeds with `experimentalApi: true`.
- A mocked `401 Unauthorized` triggers
  `account/chatgptAuthTokens/refresh`.
- The retried request uses the replacement access token.
- External auth does not create a worker `auth.json`.

Run the full local test suite:

```bash
npm test
```

Two external-auth app-server integration tests run when `CODEX_BIN` (or
`codex` on `PATH`) is executable. The third app-server integration test is the
full interrupted-turn recovery matrix; it requires `CODEX_BIN` to be an
explicit absolute path so the probe can bind evidence to that exact binary.
Unavailable integration tests are reported as skipped on Node-only CI runners;
the remaining tests still run normally.

The reference host app-server runtime currently supports macOS and Linux process
groups. A process can escape that group by creating a new session, so this is not
production containment for credential-bearing refresh. Windows is rejected
before reading managed credentials, creating a worker home, or spawning Codex;
`ChildProcess.kill()` alone is not treated as process-tree isolation.

Run the offline protocol probe and print a JSON report:

```bash
npm run probe:external-auth
```

Set `CODEX_BIN` to test a specific Codex executable:

```bash
CODEX_BIN=/path/to/codex npm test
```

A bare executable name such as `codex` is resolved through `PATH`; relative and
empty `PATH` entries are anchored to the launcher working directory before the
app-server switches into its isolated `CODEX_HOME`. A relative value containing
a path separator, such as `./bin/codex`, is likewise resolved against the
launcher working directory.

## Live External Auth Probe

The live probe reads a dedicated managed ChatGPT login from
`.test-codex-home/auth.json`, injects only its access token into a temporary
worker, and sends one fixed, non-repository prompt to the real Codex backend.
It does not pass the refresh token to the worker or modify the source auth file.

The dedicated auth home is ignored by Git. On success, the probe writes a
redacted record to `evidence/live-external-auth.json`. The tracked evidence
contains runtime metadata and the final status only; it omits credentials,
emails, complete account/workspace identifiers, and token-derived fingerprints.

Run the live probe explicitly:

```bash
npm run probe:external-auth:live
```

Optional overrides:

```bash
CODEX_TEST_HOME=/path/to/dedicated-codex-home \
CODEX_LIVE_PROBE_MODEL=gpt-5.4 \
CODEX_LIVE_EVIDENCE=evidence/live-external-auth.json \
npm run probe:external-auth:live
```

## Repository Automation

The default branch contains `.github/workflows/codex-review-gate.yml`. Pull
requests use the `codex/review-gate` check supplied by the repository template.

The retained `scripts/setup-ci.mjs` generator can add project-specific CI and
tooling later. Inspect planned writes before enabling a module:

```bash
node scripts/setup-ci.mjs --list
node scripts/setup-ci.mjs --tool github-actions --tool markdown --dry-run
```

Its generator tests are included in the default `npm test` command.

## Project Records

- Current repository state: `docs/PROJECT_STATE.md`
- Cross-workstream backlog: `docs/PROJECT_TODO.md`
- Workstream journals: `docs/project_journal/`

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.
