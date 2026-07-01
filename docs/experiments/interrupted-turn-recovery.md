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

The current probe validates Codex application semantics for classes 2 and 3. It
does not implement the production block-volume snapshot barrier.

## Reproducible Matrix

The probe runs the installed Codex app-server with a synthetic `CODEX_HOME`, a
synthetic workspace, and a held localhost Responses API server. The model
provider does not require OpenAI authentication. Each scenario uses a separate
mode `0700` temporary root and a fixed prompt that contains no repository data.

| Scenario | Termination evidence | Cold resume | Recovered tail | Next-request abort marker |
| --- | --- | --- | --- | --- |
| `logical_interrupt` | `turn/interrupt` response and `turn/completed` | same thread ID | `interrupted` | present |
| `sigterm` | child observed `SIGTERM` | same thread ID | `interrupted` | absent |
| `sigkill` | child observed `SIGKILL` | same thread ID | `interrupted` | absent |
| `snapshot_restore` | child observed `SIGKILL`; source tree quiesced before copy | same thread ID from a new absolute path | `interrupted` | absent |

A fresh app-server performs cold `thread/read {includeTurns:true}` on a private
copy of the quiesced recovery tree. A second fresh app-server performs
`thread/resume` against the original recovery tree, which the read process has
never opened. Both must report the original turn as the interrupted tail, and
neither operation may issue a model request. A completed follow-up turn is
matched by its exact turn ID and captures the single corresponding next
loopback request to distinguish a persisted abort marker from view-only
normalization.

The snapshot scenario copies the entire synthetic session tree, including
`CODEX_HOME` and workspace, after the killed process has exited. It hashes
relative paths, entry types, POSIX rwx permission bits, file bytes, and symlink
targets before and after copy. Portable UTF-8 entry names, relative symlinks,
and external absolute links are copied without following symlink targets.
Non-UTF-8 entry names, absolute links back into the source tree, relative links
whose meaning changes after relocation, special permission bits, hard-linked
files, sockets, FIFOs, and devices fail closed. The source tree
is then deleted and restored under a new absolute path; `thread/resume` receives
the restored workspace path explicitly. Its runtime `cwd` response and the
latest environment context in the follow-up model request must both resolve to
that restored directory. Immutable earlier conversation context intentionally
retains the original workspace path, and the redacted evidence records that
dual-path behavior as booleans. Cold `thread/read` independently confirms the
recovered tail in a separate app-server process.

## Live Result

The complete matrix passed on macOS arm64 with:

- Codex CLI `0.142.4`;
- binary SHA-256
  `32b3b3a3e8e19b09f2b74979ca2a7f6890dc88b8335bb0e1913a0ad68a6505b5`;
- source analysis at upstream commit
  `db887d03e1f907467e33271572dffb73bceecd6b`.

The source commit identifies the code inspected for semantics; it is not a
claim that the installed binary was built from that exact commit. The redacted
machine-readable result is stored in
`evidence/interrupted-turn-recovery.json`. It contains no thread or turn IDs,
paths, prompts, model output, credentials, account identifiers, or hostnames.

Run the compatibility probe with no real model or credential access:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery
```

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
last record. New bytes can be concatenated onto the bad tail. Production crash
restore must inspect and preserve the original, then truncate or terminate the
tail at a validated record boundary before allowing append. That repair is not
implemented by this spike.

A production clean migration checkpoint must additionally:

1. stop new turn admission;
2. confirm no active turn;
3. enumerate and terminate background terminals, then confirm they exited;
4. close stdio and wait for app-server EOF shutdown;
5. apply the host storage sync and filesystem-freeze barrier;
6. snapshot the single-attached session volume and then unfreeze it.

SQLite metadata, its WAL and shared-memory files, rollouts, workspace, and
session metadata must share the same atomic volume boundary. The later storage
contract PR will define the exact lease, fencing, manifest, snapshot, and
restore interfaces.

## Limitations

- `stopped-tree-copy` means a deterministic copy after the app-server process
  group has exited. It is not an online snapshot, block snapshot, atomic
  snapshot, or proof of power-loss durability.
- The test uses a localhost held-response mock and proves Codex compatibility,
  not real backend behavior.
- The probe covers a clean JSONL prefix after process termination. It documents
  but does not inject or repair torn JSON, a missing final newline, filesystem
  writeback loss, or inconsistent SQLite WAL state.
- Background terminal state is intentionally outside the recoverable
  filesystem contract. A checkpoint with a live terminal is not migration
  ready even if all files copy successfully.
- The implementation targets macOS and Linux process groups, while this live
  evidence records macOS arm64 only. Windows is rejected because a Job Object
  process-tree implementation is not present.
- Signal scenarios intentionally require the pinned runtime to report actual
  signal termination. If a future Codex binary traps `SIGTERM` and exits cleanly,
  the compatibility probe fails until that changed shutdown contract is reviewed.
- Portable UTF-8 directory entry names, relative symlink targets, and existing
  external absolute symlink targets are preserved exactly; non-UTF-8 names or
  targets, dangling absolute targets, internal absolute targets, and
  non-relocatable relative targets fail closed. A fixed
  runtime image must provide every external target, such as a Codex helper path,
  at a compatible location during copy and after migration.
- The stopped-tree copy does not preserve ownership, ACLs, extended attributes,
  timestamps, special permission bits, or hard-link topology. Special bits and
  hard links fail closed; the other metadata remains outside this probe and must
  be preserved by the later volume-snapshot implementation.
