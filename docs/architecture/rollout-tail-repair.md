# Rollout-Tail Repair Contract

## Status and Scope

This document defines the implemented contract for the offline rollout-tail
repair step before writable Codex resume. The executable adapter lives in
`src/rollout-tail-repair.mjs`; its unit and fault-injection coverage lives in
`test/rollout-tail-repair.test.mjs`.

The source analysis is pinned to upstream Codex commit
`db887d03e1f907467e33271572dffb73bceecd6b`. At that revision, the rollout
writer serializes one JSON value, appends one LF byte (`0x0a`), writes the full
record, and flushes its userspace buffer. It does not establish an `fsync`
durability barrier. The loader skips malformed lines, while writable resume
opens the same rollout in append mode. Consequently, both a torn final record
and a complete final record without LF can be concatenated with the next
record.

The repair step fixes only that byte-framing hazard. It does not reconstruct
conversation semantics, create an abort event, or decide that a checkpoint is
safe to resume.

## Authority and Preconditions

Repair operates on a restored writable copy. The immutable checkpoint is the
original crash evidence and is never opened for write or replaced in place.

Before invoking repair, a trusted storage and launcher-admission layer must
establish all of the following:

- the source writer has stopped or has been conclusively fenced;
- the restored volume is detached from every worker and has one trusted
  maintenance attachment;
- no app-server, background terminal, helper, or other process can write the
  restored tree;
- worker admission remains closed for the complete scan, mutation, durability,
  and readback interval; and
- the filesystem has approved local atomic-rename, file-sync, directory-sync,
  no-follow, and cache-coherence semantics.

An API option such as `stopped: true`, an expired lease, a rollout flush, or a
successful `turn/completed` event cannot prove these conditions. The
repair primitive consumes the precondition; it does not mint stopped-writer,
fence, attachment, or launcher authority. NFS and other unapproved shared or
remote filesystems remain outside this contract.

## Pinned Compatibility Adapter and Discovery

Rollout layout and schema are Codex-owned details. A version-specific adapter
must bind the repair algorithm to the audited source commit and the expected
`codex --version`. It also requires a canonical executable SHA-256 supplied by
a trusted runtime resolver and echoes that binding in the proof; the repair
primitive does not independently hash or authorize an executable.

`repairStoppedRolloutTails({ codexHome, rootSessionId, runtimeIdentity })`
accepts the exact runtime identity shape
`{ codexVersion, codexBinarySha256, sourceAnalysisCommit }`. The current
adapter accepts `codex-cli 0.144.1`, source analysis commit
`db887d03e1f907467e33271572dffb73bceecd6b`, and a structurally valid lowercase
SHA-256 binding for the platform executable. Any other version, source pin,
malformed digest, or identity shape fails before filesystem mutation.

The adapter discovers rollouts by safely enumerating the restored
`<codexHome>/sessions` tree. It may use the plain `.jsonl` suffix to select the
supported physical format, but it must not derive a pathname from a thread ID,
trust a date-directory convention, or treat SQLite or `session_index.jsonl` as
ownership authority. Each candidate's first logical record must be the pinned
adapter's `SessionMeta` form, including:

```json
{
  "type": "session_meta",
  "payload": {
    "cli_version": "0.144.1",
    "id": "<thread-id>",
    "session_id": "<root-session-id>"
  }
}
```

The actual record also contains other version-owned fields; the example shows
only the fields used for binding. Every candidate must report the pinned
`cli_version`; a missing or different writer version fails closed. Exactly one
rollout must have both `id` and `session_id` equal to the requested root thread
ID. A subagent rollout must have a distinct `id` and the same root
`session_id`. Missing or malformed metadata, duplicate or ambiguous root
ownership, duplicate thread ownership, or a foreign session fails the whole
preflight.

Enumeration is bounded to 256 rollout files, 1,024 directories, directory
depth 8, 1,280 directory entries, 1 MiB of aggregate UTF-8 entry-name bytes,
64 MiB per file, and 256 MiB of discovered input bytes in total. Directory
enumeration uses a fixed-buffer streaming `opendir()` reader and applies the
entry and name-byte budgets before retaining or sorting each name. Full-tree
and publication-time directory revalidation use the same bounded reader, so an
entry-set race cannot reintroduce an unbounded `readdir()` allocation. Each
descriptor-pinned file size is compared with the remaining aggregate budget
before a content Buffer is allocated or read. These input and enumeration
bounds are not a strict process-RSS bound: analysis and replacement readback
can briefly retain additional bounded Buffers. Candidate directories and files
must remain inside the held Codex-home authority. The 64 MiB bound also applies
to the final repaired byte sequence, so an LF append that would cross the limit
fails before a replacement is created. Plain rollout files
must be current-user-owned regular files with one link, owner read/write
permission with no execute bits, no special mode bits, and no group/world
write permission, and no extended ACL. Enumerated directories must be
current-user-owned directories with all owner permissions, no special mode
bits, no group/world write permission, and no extended ACL. This admits common Codex-created
`0644`/`0640` rollout files and `0755`/`0750` directories as inputs that can be
tightened; group/world-writable objects, owner-incomplete objects, and
owner-executable rollout files fail closed.

Before reading a rollout byte, the adapter compares path metadata with metadata
from a no-follow opened handle, uses only that pinned handle to change the mode
to exact `0600` for a file or `0700` for a directory, syncs a changed inode, and
then revalidates the path and handle identity, unchanged content metadata, and
exact private mode. It then requires the platform ACL inspector to report no
extended ACL and repeats the complete path/handle fingerprint check so an ACL
or mode race cannot hide behind the permission-tightening exception. The outer
Codex-home identity check binds the caller's original path observation to the
descriptor's pre-tightening fingerprint, so only a mode change performed by
that pinned descriptor receives the narrow `ctime`/mode exception.

Every later directory and file validation repeats the ACL check with complete
path/handle fingerprints immediately before and after inspection. Directory
validation also checks the exact expected entry set and repeats the complete
fingerprint check after enumeration. The repaired file's readback handle stays
pinned through the final full-tree pass. The repair
revalidates the parent ACL immediately before creating a replacement, checks a
new `O_EXCL` temporary file for inherited ACLs before writing rollout bytes,
rechecks the parent after the pre-rename fault window, rebinds the temporary
pathname to its held descriptor and full pre-rename fingerprint, and performs
a final ACL pass before returning. An identity race, ACL inspection failure,
or failed metadata sync fails closed. Permission tightening can occur while
the tree is discovered, but the complete content candidate set remains
validated before the first byte-framing repair.
Every successfully opened home, directory, and rollout handle is registered
with cleanup before the next identity assertion, so a failed race check does
not defer descriptor release to garbage collection.
Symlinks, hard links, FIFOs, devices, sockets, compressed `.jsonl.zst`
rollouts, unknown physical rollout formats, unsafe permissions, filesystem
device changes, and identity changes fail closed. The complete candidate set
is discovered and validated before the first content repair. The adapter
checks `st_dev` before and after no-follow opens; the external attachment
authority must additionally exclude same-device bind mounts and other mount
aliases that `st_dev` alone cannot distinguish.

Tightening a restored object's mode prevents later group/world access through
that pathname; it cannot undo disclosure that may already have occurred while a
checkpoint was stored as `0755`/`0644`. Production launchers should therefore
set a private `0077` umask before Codex creates session state, while retaining
this normalization for already captured trees.

## Exact Byte-Framing Algorithm

For each candidate, let `bytes` be its exact byte sequence and let `start` be
zero. The adapter performs these steps without normalizing whitespace or
re-serializing any retained JSON:

1. Scan from byte zero for LF (`0x0a`). For every LF at offset `i`, validate
   `bytes[start..i]` as a non-empty strict-UTF-8 JSON record. The first logical
   record must additionally pass the pinned `SessionMeta` binding above. An
   empty, whitespace-only, UTF-8-BOM-prefixed, invalid-UTF-8, or malformed
   LF-terminated record at any position is non-tail corruption and fails
   closed. After validation, set `start = i + 1`.
2. If `start == bytes.length`, the file already ends at a validated record
   boundary. Its action is `unchanged`.
3. Otherwise, `bytes[start..bytes.length]` is the only unterminated tail. If it
   is one complete, non-empty strict-UTF-8 JSON value, retain every byte and
   append exactly one LF. Its action is `append_lf`.
4. If that unterminated tail cannot be decoded or parsed as one complete JSON
   value, it is a supported torn tail only when a valid newline-terminated
   `SessionMeta` and prefix have already been established. Replace the file
   with `bytes[0..start]`; its action is `truncate_partial_tail`, and the
   removed byte count is `bytes.length - start`.
5. If the first record itself is torn, ownership cannot be authenticated and
   repair fails rather than truncating the file to zero.

Every LF-terminated record is therefore preserved byte for byte, and every
successful output ends in exactly one existing or appended record delimiter.
The algorithm never inserts `TurnAborted`, `<turn_aborted>`, a synthetic model
item, or any other semantic record. A second successful run over the same
bytes must report `unchanged`.

## Atomic Replacement, Durability, and Readback

Before planning an action, the implementation must capture a no-follow file
identity, size, and SHA-256 digest. An unchanged result still requires stable
identity and exact readback.

For either modifying action, the replacement sequence is:

1. create a private mode-`0600`, same-directory temporary regular file with
   exclusive creation and no symlink following;
2. write the exact planned bytes and sync the file;
3. revalidate the original file, parent directory, candidate set, and captured
   identity before publication;
4. atomically replace the original pathname by renaming the temporary file on
   the same approved filesystem;
5. sync the held parent directory; and
6. reopen the final pathname without following links, verify its file type,
   ownership, link count, mode, identity, size, ACL state, and SHA-256 digest
   against the planned result, and retain that handle through the final
   full-tree verification.

A failure before rename must leave the original pathname unchanged. A failure
after rename, including directory-sync or readback failure, is an uncertain
repair outcome: it must keep launcher admission closed and require a complete
trusted reinspection or a fresh restore. Cleanup must never remove a pathname
whose identity is no longer proven.

Each file replacement is atomic; repair of several rollout files is not one
cross-file filesystem transaction. A later-file failure can therefore leave
earlier files durably repaired. This is safe only because the volume remains
detached, the operation is idempotent, and no worker is admitted until a fresh
full-tree pass succeeds.

## Content-Free Proof

A successful full-tree pass returns a deterministic, immutable proof containing
only binding and framing metadata, conceptually:

```json
{
  "compatibility": {
    "codexVersion": "codex-cli 0.144.1",
    "codexBinarySha256": "<digest>",
    "sourceAnalysisCommit": "<commit>"
  },
  "rootSessionId": "<root-thread-id>",
  "files": [
    {
      "relativePath": "<relative-to-sessions>.jsonl",
      "before": { "sha256": "<digest>", "size": 123 },
      "after": { "sha256": "<digest>", "size": 124 },
      "action": "append_lf",
      "removedBytes": 0
    }
  ]
}
```

These field names are the executable contract. File order is
canonical. The proof contains no rollout bytes, decoded JSON, prompts,
responses, model output, working-directory values, Git state, or synthesized
conversation data. It is content-free, not anonymous: a root ID, relative
path, sizes, and content digests can still be sensitive metadata and must be
redacted from public evidence when required.

The proof records what the repair pass observed and durably published. It is
not a lease, fence, stopped-writer capability, checkpoint authenticity proof,
or launcher-admission token.

## Supported and Rejected Damage Classes

| Class | Result |
| --- | --- |
| All records valid and final LF present | Preserve bytes; `unchanged`. |
| Final record is complete JSON but lacks LF | Preserve bytes; `append_lf`. |
| Final unterminated record is torn after a validated prefix | `truncate_partial_tail`. |
| Malformed, empty, or invalid-UTF-8 LF-terminated record | Reject as non-tail corruption. |
| Torn or invalid first `SessionMeta` | Reject because ownership is unknown. |
| Missing, duplicate, ambiguous, or foreign session binding | Reject the full pass. |
| Compressed or unknown rollout representation | Reject; do not materialize or rewrite it. |
| Unsafe object, permissions, filesystem device, alias, or identity race | Reject and keep admission closed. |

Syntactically valid but semantically altered JSON, records lost before capture,
SQLite/WAL inconsistency, filesystem writeback loss outside the captured
prefix, and application-level turn reconstruction are not detectable by this
framing repair.

## Same Executable and OCI Same-Image Claims

The compatibility probe executes a private copy of one Codex binary
before and after repair and requires the same version and executable SHA-256.
That proves a same-pinned-executable experiment. The audited upstream commit
identifies source used to understand behavior; it does not prove that the
binary was built from that commit.

Production `same-image` is a stronger claim. It requires the trusted resolver
and launcher to match the session manifest's exact platform OCI manifest
digest, media type, platform, and measured Codex version before admission. An
executable digest alone does not cover the image filesystem, helpers,
libraries, configuration, or platform descriptor. Until that resolver and
launcher conformance work lands, evidence must use the narrower
same-pinned-executable wording.

The tracked schema-v6 probe exercises both modifying actions after `SIGKILL`
on a writable copy produced by the probe's stopped-tree-copy helper, resumes by
explicit thread ID, completes a follow-up turn, and confirms that exact turn
through a third fresh app-server cold read while the immutable backup digest
remains unchanged. It does not exercise a production backend restore.

After successful repair, the intended recovery path is explicit
`thread/resume { threadId, cwd }` followed by a completed turn and a cold
readback. The unstable rollout `path` override is not part of the contract.
The production launcher must retain the admission guard until repair proof and
runtime-image resolution have succeeded, then atomically re-read canonical
lease, fencing, attachment, and manifest authority while starting the worker.

## Explicit Non-Goals

This work does not implement or claim:

- checkpoint capture, mutation of the original immutable checkpoint, or an
  atomic multi-file repair transaction;
- production lease, fencing, reservation, catalogue, or launcher-admission
  authority;
- an OCI resolver, image signature policy, ext4 or filesystem-image backend,
  NFS repair semantics, or cross-host migration;
- SQLite/WAL recovery, semantic rollout validation, abort-marker synthesis,
  fork/rollback truncation, or background-terminal migration;
- compressed-rollout repair, differential compression, content-addressed
  storage, encryption, retention, or periodic long-goal snapshots;
- authentication state handling; or
- Git Summary. Git Summary remains deferred read-only user context and is not
  recovery authority.
