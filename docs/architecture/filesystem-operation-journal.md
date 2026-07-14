# Durable Filesystem Operation Journal

## Scope

The filesystem operation journal provides a host-local durable state machine
for one exact storage operation. It records the operation binding, storage
mutation request, complete checkpoint descriptor, predetermined final result,
and later materialisation metadata without performing the storage operation
itself.

`FilesystemOperationJournal` exposes four authoritative operations:

- `read()` observes the canonical record or the implicit `absent` state;
- `prepare()` creates the first exact operation record;
- `markMaterialized()` records caller-supplied materialisation metadata; and
- `commit()` authorises replay of the exact result fixed by `prepare()`.

`readStateHint()` is a separate read-only planning primitive. It opens and
validates the canonical record without acquiring the journal lock and without
running observable journal fault callbacks. Trusted directory-ACL and
temporary-record inspectors still execute to validate the read. Publication
uses that hint only after acquiring its preprovisioned publication-root lock,
to decide whether the caller's current source leaf must be inspected before an
authoritative journal read. A hint never authorises replay or a state
transition: the caller must perform an authoritative locked `read()` and prove
that the durable state did not move backwards before relying on it. Every
transition for a publication-owned operation ID must go through the
publication layer and therefore hold that same publication-root lock. A
forward transition outside that ownership boundary is unsupported.

`describeAuthority()` is a non-transitioning integration helper for the local
publication layer. It returns the frozen canonical journal-directory path and
device/inode identity from the same pinned authority used by transitions, so
the physical layer can reject a journal nested inside a captured or published
tree. That absolute path is host-local topology data and is never persisted in
an operation record or checkpoint artefact.

`operationJournalRecordFilename()` maps an operation ID to its canonical
record filename. The mapping is journal metadata, not an artefact locator or a
portable checkpoint reference.

`OPERATION_JOURNAL_RECORD_VERSION` exposes the current canonical record schema
version. `OperationJournalError` is the public error type described under
Failure and Restart Semantics; constructing that type in an injected
collaborator does not let the collaborator forge an internally trusted journal
outcome.

`OPERATION_JOURNAL_LOCK_NAME` exposes the fixed `.operation-journal.lock`
provisioning name. Before the journal directory is exposed to a worker or used
by a journal instance, the trusted provisioner creates that empty regular file
with one link, runtime effective-UID ownership, and exact mode `0600`, then
fsyncs both the file and journal directory. Existing directories are upgraded
only while detached and quiescent. Journal calls acquire the inode with
`requireExisting: true`; they never create, chmod, truncate, unlink, or repair
it. A missing or unsafe lock fails closed as journal I/O failure.
An injected `acquireLock(path, { requireExisting: true })` implementation is a
trusted adapter and must preserve that existing-only, no-mutation contract.

Reads, state hints, and transitions return a deeply frozen
`{ record, replayed }` envelope.
An absent read has `record: null`; a transition sets `replayed: true` only when
it returns an already-published exact state without rewriting the record.

Every canonical record has exactly these fields:

```text
recordVersion, operationId, revision, state, binding, request, result,
materialization
```

`binding`, `request`, and the predetermined `result` are present from the first
prepared record and remain byte-exact through every later revision.

## State Machine

The only forward sequence is:

```text
absent -> prepared (revision "1", materialization null)
       -> materialized (revision "2", materialization object)
       -> committed (revision "3", same materialization object)
```

No transition may skip or move backwards. In particular, `prepared` cannot
become `committed` without an exact `materialized` record. A caller asking for
an earlier state after another caller has already advanced the same exact
operation observes the later canonical state; the journal never rewrites a
later state back to the requested earlier phase.

The first `prepare()` fixes and durably records the complete operation binding,
request, checkpoint descriptor, and final `{ checkpoint, mutation }` result
before physical work begins. This includes the mutation proof ID and status.
The predetermined result must echo the fixed descriptor and match the
operation, session, backend, target, and request semantics. `materialized` and
`committed` preserve that complete result byte-for-byte; neither phase may
allocate or replace result fields. An exact retry returns the existing
canonical state and result without rewriting the record.

Reuse of an operation ID with any different binding, request, descriptor,
predetermined result, or materialisation metadata fails as a conflict. For
checkpoint capture, descriptor storage and source epoch must match the capture
request and writer fence. For restore, the descriptor may identify a different
source storage and source epoch from the destination request, but source and
destination must retain the session and backend relationship required by the
snapshot core.
The restore request fence must also be strictly newer than the checkpoint's
source fence.

## Canonical Durable Publication

Each state is a canonical, strictly validated plain-data record. Unknown,
missing, accessor, proxy, non-enumerable, or otherwise non-canonical fields
fail closed. Secret-bearing key names and recognised token forms are rejected;
this includes generic token fields, API-key fields, common provider token
prefixes, bearer credentials, and private-key markers. Credentials never
belong in an operation binding or result. The record version is inspected
before the v1 field set, so a future schema is reported as unsupported rather
than corrupt. JSON property order and bytes are deterministic. One normalised
operation envelope shares a
maximum budget of 8,192 value nodes and 512 KiB of cumulative canonical
component bytes across its request, binding, result, and materialisation; each
component also has a maximum nesting depth of 24. These limits are enforced
before final JSON serialisation, and the complete persisted record has its own
512 KiB byte limit. The canonical record is a mode `0600`,
single-link regular file inside a private journal directory. The first
successful use of a journal instance pins that directory's canonical path and
device/inode identity for every later call. Symlinks, hard links, unsafe
permissions, unsafe ACLs, directory identity changes, and record replacement
are rejected.

All calls in one process are serialized by the pinned journal directory's
device/inode identity, not by pathname or operation ID, because the directory
has one advisory lock. This also coalesces aliases such as bind-mount paths and
keeps independent operation IDs from turning ordinary local concurrency into a
non-retryable lock conflict. Cross-process callers still require the same
single-writer coordination boundary and preprovisioned default advisory lock.

Every forward transition uses the same publication protocol while holding the
journal lock and directory authority:

1. read and validate the current canonical state;
2. create the operation's deterministic same-directory private temporary path
   with `O_EXCL`, `O_NOFOLLOW`, and mode `0600`;
3. write and fsync the complete next canonical JSON record, retaining its open
   file descriptor and device/inode identity;
4. finish the pre-rename hooks and lock callbacks, revalidate the directory,
   and prove the temporary pathname still names that fsynced file;
5. prove the canonical pathname is still absent or still names the exact
   device/inode and byte-for-byte predecessor confirmed in step 1;
6. ask the default lock holder to recheck that destination identity immediately
   before renaming the candidate over the canonical record;
7. prove the canonical pathname still names the held fsynced file, then fsync
   the held parent directory;
8. read back the canonical record with the expected identity, revalidate it,
   and close the held file descriptor; and
9. only then return the new state.

The record becomes a successful durable journal transition only after parent
sync and canonical readback. A same-state exact replay performs no rewrite.

## Failure and Restart Semantics

Every public failure is a frozen `OperationJournalError` with a fixed `code`, a
`commitState` of `"committed"`, `"not-committed"`, or `"uncertain"`, and
`retryable: false`. The public codes are:

- `invalid_journal_request`, `invalid_journal_directory`,
  `invalid_journal_record`, and `unsupported_journal_record` for rejected input
  or durable state;
- `operation_conflict` and `invalid_state_transition` for operation-state
  mismatches;
- `journal_io_failed` for a failure known not to have committed a transition;
- `journal_commit_outcome_uncertain` when rename may have occurred but durable
  confirmation did not complete;
- `journal_recovery_required` when retained recovery evidence blocks safe
  inspection; and
- `journal_lock_release_failed` when lock release or final directory-handle
  close fails.

A failure proven to occur before rename is definitely not committed. If the
failure happens after a temporary record has been created, the journal retains
that file as recovery evidence rather than deleting it speculatively. Any later
`read()` or transition for the same operation then fails with
`journal_recovery_required` before returning an absent or canonical record.
The deterministic per-operation path makes this check a direct lookup rather
than a scan whose cost grows with the journal's permanent record history.
There is deliberately no public cleanup or automatic-recovery API in this
slice: a trusted operator recovery path must inspect and resolve the retained
temporary record before the operation can continue.
If the default lock path detects that the destination changed after the initial
state-machine decision but before rename dispatch, the transition fails as
`journal_io_failed` with `commitState: "not-committed"`; that detected pathname
is not overwritten or rolled back. The retained candidate then makes
subsequent access require recovery. A non-cooperating same-UID race after the
holder's final check remains outside the guarantee described below.

Once rename may have occurred, loss of rename acknowledgement,
parent-directory sync failure, readback failure, or post-rename lock loss is
reported as `journal_commit_outcome_uncertain` with `commitState: "uncertain"`.
A later error must not downgrade that commit-state classification to an
ordinary I/O failure or claim that the old state remains canonical. Lock-release
and final handle-close failures instead use `journal_lock_release_failed` and
preserve the current call's prior `commitState`: it may be `"not-committed"`,
`"uncertain"`, or `"committed"`. In particular, a failure releasing the lock
after a newly published transition has passed parent sync and exact readback is
a committed lock-release failure, not an uncertain journal commit.

After restart, `read()` exposes only the canonical record visible through the
validated journal directory. Before returning a visible record it pins and
fsyncs that exact file handle, fsyncs the held parent directory, and performs an
identity-bound exact readback. A record left visible by earlier parent-sync
uncertainty is therefore either durably confirmed or remains an uncertain
failure. A read may reveal `prepared`, `materialized`, or `committed` after a
caller previously observed uncertainty. Every phase retains the predetermined
exact result, while only a committed record authorises it for backend replay.
An earlier state tells the composed backend which durable journal phase was
reached. The journal does not itself continue, roll back, or reconcile the
physical operation.

`readStateHint()` deliberately omits the journal lock, fault-callback
execution, record fsync, parent fsync, and durable readback performed by
`read()`. It is
safe only inside an outer serialization boundary that owns all relevant state
transitions, as a monotonic planning hint followed by an authoritative locked
read. A temporary-record conflict, malformed record, directory-authority
change, or other ambiguous observation fails closed instead of guessing a
state.

## Explicit Non-Guarantees

The journal proves only the durable canonical state of an exact operation
record. It does not prove:

- that an artefact or restore destination was physically materialised;
- that a writer stopped or that stopped-writer evidence is authentic;
- that a lease or fencing epoch is still canonical;
- that a checkpoint or restore was atomically published;
- that a restore destination was detached or isolated;
- that NFS, a shared filesystem, or a remote provider has equivalent lock,
  rename, fsync, or cache-coherence semantics;
- that a non-cooperating process with the same UID cannot race the final
  destination check and POSIX rename; or
- that the backend operation succeeded.

Injected lock, fault, ACL, recovery-path inspection, and directory-sync
collaborators are
trusted test seams. Production publication uses the default branded advisory
lock holder. Its destination recheck narrows the final window and prevents a
stale predecessor from being overwritten after callbacks complete, but it is
not a portable kernel compare-and-swap primitive. Exclusive single-writer
control and the private-directory boundary remain required.

The materialisation metadata and predetermined result remain caller-supplied
claims when this journal is used alone. The stopped-directory publication layer
now connects those phases to a held local-directory storage barrier,
deterministic staging, absent-destination rename, parent sync, and exact final
readback. A consumer still must use that higher layer rather than infer physical
success from a journal record alone. See
`stopped-directory-publication.md`.

The stopped-directory backend now composes the same-process stopped-writer
capability, publication, durable canonical-fence authority, and the snapshot
core. The journal remains evidence rather than authority when used outside
that composition. See `stopped-directory-backend.md`.
