# Interrupted-Turn Recovery Experiment

## Decision

Treat Codex session recovery as three distinct checkpoint classes:

1. `clean`: app-server has stopped accepting work, active turns and background
   terminals are absent, stdio reaches EOF shutdown, and the storage layer has
   completed its own sync and snapshot barrier.
2. `graceful-abort`: stable `turn/interrupt` completed and persisted an explicit
   abort event before the storage barrier.
3. `crash-prefix`: the process or host stopped without a terminal turn event;
   recovery may normalize a stale in-progress view, but it must not claim the
   same durable conversation semantics as explicit interruption.

The current probe validates Codex application semantics for classes 2 and 3
and the offline byte-framing repair required before writable crash-prefix
resume. It does not implement production storage capture, sync/freeze, or
publication barriers.

## Reproducible Matrix

The probe runs the installed Codex app-server with a synthetic `CODEX_HOME`, a
synthetic workspace, and a held localhost Responses API server. The model
provider does not require OpenAI authentication. Each scenario uses a separate
mode `0700` temporary root beneath the same scratch root whose ownership,
ancestor chain, and ACL state were validated before the private Codex binary was
staged. The fixed prompt contains no repository data.

| Scenario | Termination evidence | Cold resume | Recovered tail | Next-request abort marker |
| --- | --- | --- | --- | --- |
| `logical_interrupt` | `turn/interrupt` response and `turn/completed` | same thread ID | `interrupted` | present |
| `sigterm` | child observed `SIGTERM` | same thread ID | `interrupted` | absent |
| `sigkill` | child observed `SIGKILL` | same thread ID | `interrupted` | absent |
| `snapshot_restore` | child observed `SIGKILL`; source tree quiesced before copy | same thread ID from a new absolute path | `interrupted` | absent |
| `missing_final_lf_repair` | child observed `SIGKILL`; final LF removed only on a stopped-tree-derived writable copy | explicit same thread ID and restored `cwd` after `append_lf` | `interrupted`, then one completed follow-up | absent |
| `torn_unterminated_tail_repair` | child observed `SIGKILL`; invalid unterminated bytes appended only on a stopped-tree-derived writable copy | explicit same thread ID and restored `cwd` after `truncate_partial_tail` | `interrupted`, then one completed follow-up | absent |

A fresh app-server performs cold `thread/read {includeTurns:true}` on a private
copy of the quiesced recovery tree. A second fresh app-server performs
`thread/resume` against the original recovery tree, which the read process has
never opened. Both must report the original turn as the interrupted tail, and
neither operation may issue a model request. A completed follow-up turn is
matched by its exact turn ID and captures the single corresponding next
loopback request to distinguish a persisted abort marker from view-only
normalization.

The two repair scenarios first copy the quiesced killed-process tree to an
immutable backup, copy that backup to a distinct writable restore tree, and
only then inject deterministic tail damage. They invoke
`repairStoppedRolloutTails` exactly once, resume with explicit `threadId` and
`cwd`, complete exactly one follow-up model turn, and stop. A third fresh
app-server performs `thread/read {includeTurns:true}` and must find that exact
follow-up turn still completed without issuing another model request. The
backup modeled-tree digest is checked before repair and after readback and must
remain unchanged. Repair never synthesizes an abort record or model marker.

The snapshot scenario copies every snapshot-user-accessible entry in the
synthetic session tree, including `CODEX_HOME` and workspace, after the killed
process has exited. It hashes relative paths, entry types, regular-file and
directory POSIX rwx permission bits, file bytes, and symlink targets before and
after copy. NFC-normalized portable UTF-8 entry names without non-ASCII cased
characters or collisions under ASCII lowercase comparison, existing
relocatable relative symlinks, and external absolute links are copied without
following symlink targets. Non-NFC names, unsupported cased names, name
collisions, non-UTF-8 entry names, inaccessible entries, absolute links back
into the source tree,
dangling relative links, relative-link case or normalization aliases, relative
link traversal through non-directories, resolution chains that leave the source
tree, absolute resolution chains that enter the source or destination tree,
external filesystem-identity aliases into the source tree,
relative links whose meaning changes after relocation, special permission bits,
hard-linked files or symlinks, sockets, FIFOs, and devices fail closed.
The source tree
is then deleted and restored under a new absolute path; `thread/resume` receives
the restored workspace path explicitly. Its runtime `cwd` response and the
latest environment context in the follow-up model request must both resolve to
that restored directory. Immutable earlier conversation context intentionally
retains the original workspace path, and the redacted evidence records that
dual-path behavior as booleans. Cold `thread/read` independently confirms the
recovered tail in a separate app-server process.

The stopped-tree implementation now lives in `src/stopped-tree.mjs`. The probe
imports and re-exports the existing parsing, pathname, copy, digest, and cleanup
API, so the extraction does not change this experiment's compatibility claim
or failure semantics. The reusable module remains non-durable; see
`docs/architecture/stopped-tree-primitives.md`.

## Live Result

The complete matrix passed on macOS with an arm64 Node launcher and:

- Codex CLI `0.144.1`;
- execution from a private mode `0500`, single-link copy whose file type, link
  count, mode, and digest are checked before every scenario and after the full
  matrix;
- binary SHA-256
  `29915529b97697def1a957b0505e770aa6a45744435d62fc263e98d7619e167a`;
- source analysis at upstream commit
  `db887d03e1f907467e33271572dffb73bceecd6b`.

The source commit identifies the code inspected for semantics; it is not a
claim that the installed binary was built from that exact commit. Reusing the
same staged private executable for all six scenarios proves only the stated
`same-pinned-executable` compatibility, not OCI `same-image`. The redacted
machine-readable result is stored in
`evidence/interrupted-turn-recovery.json`. It contains no thread or turn IDs,
paths, prompts, model output, credentials, account identifiers, or hostnames.
Schema version 6 records the private binary execution mode, the
`same-pinned-executable` compatibility claim, the Node launcher architecture
(not an inferred Codex binary architecture), the exact
`copy-original-path-absent-held-tree-000` cold-read isolation mode, and the
distinction between configured inputs and OS-enforced isolation; older evidence
is rejected. Repair scenarios retain only action names, counts, and booleans;
their internal proof paths, thread IDs, content hashes, byte sizes, and rollout
content are discarded. The runtime section retains the existing Codex version
and executable SHA-256 binding.
Snapshot fields use `modeledTreeDigestMatched` terminology because the digest
intentionally excludes metadata listed under limitations.

Run the compatibility probe with no credential input and a loopback model
provider:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery
```

When `--write-evidence` is selected, the evidence parent directory must already
exist, be owned by the current user, grant owner read/write/search access, deny
group/world writes, and have no extended ACL. Its canonical ancestor chain must
be current-user/root-owned and non-writable except for sticky shared ancestors,
with no unsafe ACLs. Publication holds an `O_DIRECTORY|O_NOFOLLOW` handle,
revalidates directory, temporary-directory, and file identities around rename,
and fsyncs the held directory handle. A failed publication retains its private
temporary artifact for trusted-owner inspection when rename has not occurred.
After rename, a directory-sync failure leaves the destination in place and the
CLI reports `evidence_durability_uncertain` without exception details; the
caller must not treat that path as durable evidence without inspection or a
later successful publication. The
helper never recursively removes a pathname that may have been replaced.
Concurrent mutation by another process with the same UID is outside this
pure-Node helper's security boundary.

If the default temporary filesystem is mounted `noexec`, set
`CODEX_RECOVERY_EXEC_ROOT` to an existing absolute directory on an executable
filesystem. The root must be owned by the current user or root; if it is
group/world-writable, it must have the sticky bit. The complete canonical
ancestor chain must be owned by the current user or root, non-writable except
for sticky shared ancestors, and free of unsafe extended ACLs; the root itself
must have no extended ACL. The probe creates a mode `0700` subdirectory there,
executes a mode `0500` private Codex copy, and removes the subdirectory after
the matrix. This override is execution scratch only and must not point into the
recoverable session tree.
The pinned macOS runtime must provide ACL-aware `/bin/ls` and `/sbin/mount`;
Linux must provide ACL-aware `/usr/bin/getfacl` and
`/proc/self/mountinfo`. The probe invokes these inspection surfaces through
fixed paths and arguments and fails closed on missing, malformed, or
incomplete output. BusyBox/Toybox `ls` output is not accepted as Linux ACL
evidence. Other runtime layouts are outside this probe's compatibility claim
until they provide equivalent pinned inspectors.

## Upstream Semantics

Source analysis found the following behavior in the pinned analysis commit:

- `turn/interrupt` validates the active thread and turn, submits
  `Op::Interrupt`, resolves the RPC after core emits `TurnAborted`, and then
  emits `turn/completed` with status `interrupted`.
- Explicit interruption records the model-visible `<turn_aborted>` item, flushes
  the rollout, emits the terminal abort event, and flushes again.
- Cold `thread/resume` and `thread/read` normalize an incomplete persisted tail
  from `inProgress` to `interrupted` when no live turn exists.
- Hard-kill recovery does not synthesize or persist a missing abort marker or
  terminal abort event. The normalized API view is therefore weaker evidence
  than an explicit interrupt.
- Interrupting a turn does not guarantee that unified-exec background terminals
  stop. Their process and PTY state is held in memory and cannot move through a
  filesystem snapshot.

Relevant upstream files include:

- `codex-rs/app-server/src/request_processors/turn_processor.rs`
- `codex-rs/app-server/src/bespoke_event_handling.rs`
- `codex-rs/core/src/tasks/mod.rs`
- `codex-rs/core/src/context/turn_aborted.rs`
- `codex-rs/app-server-protocol/src/protocol/thread_history.rs`
- `codex-rs/app-server/src/request_processors/thread_lifecycle.rs`
- `codex-rs/app-server/src/request_processors/thread_processor.rs`
- `codex-rs/rollout/src/recorder.rs`
- `codex-rs/core/src/unified_exec/process_manager.rs`

## Storage Consequences

Codex rollout `flush()` currently performs buffered file `write_all` and
`flush`; it does not call `sync_data`, `sync_all`, `fsync`, or `syncfs`. A
successful turn-boundary notification is therefore a logical checkpoint, not a
power-loss durability barrier.

The rollout loader skips an invalid JSONL tail, but resume opens the same file
for append without first repairing a truncated or complete-but-unterminated
last record. New bytes can be concatenated onto the bad tail. The offline
`repairStoppedRolloutTails` primitive now validates the pinned rollout set and
either appends one missing LF or truncates only an invalid unterminated tail on
a restored writable copy before resume. It preserves the immutable backup and
does not reconstruct conversation semantics. Production still must supply the
stopped-writer, attachment, durability, and launcher-admission authority around
that primitive.

A production clean migration checkpoint must additionally:

1. stop new turn admission;
2. confirm no active turn;
3. enumerate and terminate background terminals, then confirm they exited;
4. close stdio and wait for app-server EOF shutdown;
5. apply the host storage sync and filesystem-freeze barrier;
6. snapshot the single-attached session volume and then unfreeze it.

SQLite metadata, its WAL and shared-memory files, rollouts, workspace, and
session metadata must share the same atomic volume boundary. The existing
storage contracts define these logical boundaries; concrete production
adapters, physical capture, and launcher admission remain pending.

## Limitations

- `stopped-tree-copy` means a deterministic copy after the app-server process
  group has exited. It is not an online snapshot, block snapshot, atomic
  snapshot, or proof of power-loss durability.
- The test uses a localhost held-response mock and proves Codex compatibility,
  not real backend behavior.
- The probe provisions an isolated `CODEX_HOME`, an allowlisted worker
  environment, and a provider with `requires_openai_auth = false`; it does not
  prove that the app-server process could not read unrelated host files or make
  unrelated outbound connections. Use container-level filesystem and network
  isolation when that stronger claim is required.
- The repair matrix covers one complete final JSON record missing LF and one
  invalid unterminated tail after a valid prefix. It does not cover filesystem
  writeback loss, syntactically valid semantic corruption, or inconsistent
  SQLite WAL state.
- Background terminal state is intentionally outside the recoverable
  filesystem contract. A checkpoint with a live terminal is not migration
  ready even if all files copy successfully.
- The implementation targets macOS and Linux process groups, while this live
  evidence records a macOS host and arm64 Node launcher only. Windows is
  rejected because a Job Object process-tree implementation is not present.
- Signal scenarios intentionally require the pinned runtime to report actual
  signal termination. If a future Codex binary traps `SIGTERM` and exits cleanly,
  the compatibility probe fails until that changed shutdown contract is reviewed.
- NFC-normalized portable UTF-8 directory entry names, existing relocatable
  relative symlink targets, and existing external absolute symlink targets are
  preserved exactly; entries inaccessible to the snapshot user, non-NFC names,
  non-ASCII cased names, case-insensitive name collisions, non-UTF-8 names or
  targets, dangling absolute or relative targets, internal absolute targets,
  relative-link case or normalization aliases, traversal through
  non-directories, relative resolution chains that leave the source tree,
  absolute resolution chains that enter the source or destination tree,
  external filesystem-identity aliases into the source tree, and
  non-relocatable relative targets fail closed. A fixed
  runtime image must provide every external target, such as a Codex helper path,
  at a compatible location during copy and after migration. Symlink resolution
  chains longer than the common macOS/Linux limit of 32 are rejected.
- The stopped-tree copy does not preserve ownership, ACLs, extended attributes,
  timestamps, symlink permission bits, special permission bits, or hard-link
  topology. Special bits and hard links, including hard-linked symlinks, fail
  closed; the other metadata remains outside this modeled digest and must be
  preserved by the later volume-snapshot implementation.
- The stopped-tree copy requires exclusive single-writer control of its
  current-user-owned, mode `0700`, extended-ACL-free root. Missing or malformed
  ACL inspection fails closed. It holds the root, validates its complete
  ancestor chain as a trusted authority, and revalidates root, ancestor, and
  destination identities during the copy. Concurrent mutation by another
  process with the same UID is outside its security contract. The production
  design supplies this invariant with session fencing, worker quiescence, and
  a single-attached volume before snapshot.
- Digest, copy, and pathname cleanup reject a root that is itself a mount point
  or contains a nested mount point. Linux mount boundaries, including
  same-device bind mounts, come from `/proc/self/mountinfo`; macOS boundaries
  come from the fixed `/sbin/mount` table. Because macOS prints raw mount paths,
  entries containing text that is ambiguous with its output separators fail
  closed. Both tables are read as bytes and fail closed before parsing if
  strict UTF-8 decoding would be lossy. Production cleanup destroys the fenced
  session volume instead of recursively deleting an unknown mounted tree.
- If validation or copy fails after the destination directory is created, the
  partial destination is retained. Pathname-based recursive cleanup cannot
  atomically prove that a racing writer has not replaced that directory, so the
  helper leaves cleanup to a trusted owner that can tear down the enclosing
  owned root. The production volume-snapshot implementation must provide its
  own atomic discard operation.
