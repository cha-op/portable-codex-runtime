# Snapshot and Restore Core

## Scope

The snapshot and restore core is backend-neutral orchestration for `clean`
checkpoints taken only after the writer has stopped. It consumes the portable
session storage contracts, validates the complete operation boundary, delegates
the physical mutation to a storage backend, and returns a portable checkpoint
descriptor only after a definite successful result.

This first slice deliberately supports only:

- `checkpointClass: "clean"`;
- `writerBoundary: "stopped"`; and
- `captureBoundary: "storage-barrier"`.

The caller must establish those facts before invoking the core and supply an
opaque `stoppedWriterEvidence` handle for capture. The core verifies only that
the handle has a safe opaque envelope and passes it through unchanged. The
backend must authenticate and atomically bind the handle to its stop and
storage-barrier authority. A declaration in a request, or merely supplying an
arbitrary object, is not evidence that a process stopped or that the
filesystem is quiescent.

The same-process stopped-writer coordinator supplies the concrete handle for
the stopped-directory backend. It authenticates the exact original object by
private `WeakMap` identity and permits one capability to wrap one snapshot
callback. The handle cannot be reconstructed from serialized data and is never
portable checkpoint metadata. See `stopped-writer-capability.md`.

`turn/completed`, `ShutdownComplete`, `thread/closed`, thread unsubscribe, and
rollout flush are not writer-stop proofs. Production issuance requires a
fully joined container, cgroup, or VM writer boundary, or a future Codex
shutdown result that propagates failures and joins every persistence writer.

The core does not stop writers, prove quiescence, attach or start a worker,
resume a Codex thread, or implement a physical storage backend. It does not
turn the checkpoint descriptor into lease or fencing authority.

The reusable stopped-tree primitives provide validated copy, digest, and
guarded-cleanup mechanics for an already stopped directory tree. They do not
satisfy the backend obligations below: in particular, they provide no fsync
barrier, atomic publication, operation journal, or durable replay. See
`stopped-tree-primitives.md`.

The durable filesystem operation journal can fix and replay the exact request,
checkpoint descriptor, materialisation metadata, and committed result across
process restart. It does not prove that any physical backend step occurred and
does not satisfy the core's stop, fence, barrier, publication, or destination
isolation obligations. See `filesystem-operation-journal.md`.

The stopped-directory publication layer supplies the local physical barrier,
checkpoint bundle or restore-tree staging, atomic final-name publication, and
exact readback boundary. It still does not authenticate stopped-writer
evidence, atomically recheck a canonical fence, or implement the backend
contract. See `stopped-directory-publication.md`.

The stopped-directory backend composes that physical layer with the
same-process capability and a durable mutation-authority/catalogue seam. Its
authority must reserve the exact predetermined result, hold the canonical
fence and admission guard across publication, and durably finalize before
success. Its optional capture-reconciliation extension authenticates a durable
attempt and verifies only an already committed artefact without another writer
stop. It is a manual-fencing local-filesystem backend; the production authority
database remains separate work. See `stopped-directory-backend.md`.

`captureCleanCheckpoint()`, `reconcileCleanCheckpointCapture()`, and
`restoreCleanCheckpoint()` are the orchestration entry points. All three reject
`graceful-abort` and `crash-prefix` before backend dispatch. Successful results
contain the deeply frozen validated checkpoint descriptor and exact storage
mutation result; neither result gains new authority fields.

## Orchestration Boundary

For capture, the core validates the immutable session manifest, portable
storage reference, host-local attachment, canonical lease snapshot, and exact
capture request. The records must agree on the runtime session, storage,
attachment, writer identity, and fencing epoch. It then invokes the selected
backend with the immutable checkpoint descriptor constructed from those
validated inputs. The core returns that descriptor together with the validated
mutation result only after backend success is definite.

The capture backend receives the validated
`{ attachment, checkpoint, request, stoppedWriterEvidence }` record. The
restore backend receives `{ checkpoint, request }`. These are structural
snapshots and an opaque evidence capability for the backend transaction, not
evidence that an earlier core comparison remains current.

Capture reconciliation also receives `{ checkpoint, request }`, but only after
the core validates the optional backend extension and proves that the original
checkpoint request exactly names the descriptor's backend, storage, session,
source fencing epoch, checkpoint ID, and artefact ID. It intentionally performs
no current-lease or expiry check: the recovery use case begins after the old
lease may have expired or authority may have moved. The backend's trusted
mutation authority, not those serialized fields, must load the canonical
durable attempt and authorize committed-result verification.

Both backend operations must return the exact plain-data
`{ checkpoint, mutation }` result envelope. The echoed checkpoint must match
the complete dispatched descriptor, including `createdAt`, image identity,
source storage, and source fence. The core treats a missing or mismatched echo
as post-dispatch uncertainty. This prevents a durable operation-ID replay from
being paired with newly generated descriptor metadata.

For restore, the core validates the manifest, source checkpoint descriptor,
destination storage, canonical lease snapshot, and exact restore request. The
source and destination must belong to the same session and backend in this
slice, but the destination may be a replacement storage ID. The target writer
fencing epoch must be strictly greater than the checkpoint's source epoch.
Epochs are compared as canonical uint64 integers, not JavaScript `Number`
values. A restored descriptor or source epoch never becomes canonical writer
authority.

The core performs structural validation and orchestration. It does not split a
fence check from a later physical mutation and call that atomic. The backend
must perform the authoritative work within its own transaction or equivalent
provider operation.

## Backend Obligations

Before returning success on any path, a concrete backend must:

- bind the operation ID to the exact session, storage, checkpoint or artefact
  target, complete checkpoint descriptor, and writer tuple;
- for capture, bind that operation to the exact attachment ID and attachment
  proof supplied by the validated attachment snapshot, authenticate and consume
  the stopped-writer evidence handle, and bind it to the same callback; and
- return the exact checkpoint descriptor and durable mutation result for that
  binding.

A new or resumable physical mutation path must additionally:

- atomically recheck the complete canonical writer fence against the mutation;
- prove that a restore destination is detached and isolated before mutation;
- establish the required storage barrier for clean capture; and
- perform the physical capture or restore and durably commit its proof.

A normal clean-capture dispatch must atomically start a fresh durable operation
inside its capability-consumption callback. It may not adopt or advance an
earlier `prepared`, `materialized`, or `committed` publication: journal
bindings and serialized correlation IDs do not prove that the earlier bytes
were captured after the current writer stop. Such state makes the current
writer and capability terminal and stays blocked for the later authenticated
reconciliation path. The backend captures one designated coordinator in
trusted state and does not instantiate a replacement coordinator as a retry
mechanism.

Restore may replay an exact committed destination publication because the
current path separately proves a newer destination fence, detached isolation,
and a trusted immutable checkpoint proof. Capture replay requires an
authenticated durable attempt-provenance record plus committed-object
verification; a journal record alone is not replay authority. The
reconciliation extension supplies that narrow path and cannot create, copy,
rename, materialize, or commit an artefact.

The backend, rather than the core, defines the physical checkpoint and restore
mechanism. A filesystem copy, image snapshot, reflink, volume-provider
snapshot, or remote artefact is trustworthy only after its adapter satisfies
the same conformance contract.

## Failure Model

Validation failures before backend dispatch do not authorise or imply a
storage mutation. Once dispatch may have occurred, every timeout, transport
failure, malformed response, mismatched result, or otherwise uncertain outcome
fails closed. The core does not synthesise success, emit a checkpoint
descriptor, retry under a different operation ID, or permit writable resume
from an uncertain result.

Post-dispatch capture and restore uncertainty is exposed only as the fixed,
non-retryable `checkpoint_outcome_uncertain` and
`restore_outcome_uncertain` classes. Backend exception details and credentials
are not copied into public errors.

Reconciliation uncertainty is exposed only as the fixed, non-retryable
`checkpoint_reconciliation_outcome_uncertain` class. The core dispatches the
exact original request once and never falls back to normal capture, changes the
operation ID, supplies a replacement capability, or infers success from a
checkpoint descriptor. A backend without the versioned optional extension is
rejected before dispatch.

## Explicitly Deferred Work

This core does not yet provide:

- evidence for a graceful `turn/interrupt` abort boundary;
- atomic `crash-prefix` capture or rollout-tail repair;
- repair or automatic continuation of `prepared` or `materialized` capture
  attempts;
- an ext4 or filesystem-image physical backend;
- differential compression, content-addressed storage, encryption, retention,
  or atomic remote publication;
- scheduler integration or periodic checkpoints for long-running goal turns;
- cross-host end-to-end restore and migration verification; or
- the read-only Git Summary.

These are separate evidence and backend workstreams. In particular, Git state
is user context and is not part of checkpoint correctness.

## Dependency History and Remaining Order

The completed foundations and remaining storage work follow the serial
pull-request order in the runtime delivery plan:

1. PR #6, backend-neutral snapshot and restore core (completed);
2. PR #7, reusable stopped-tree primitives (completed);
3. PR #8, durable filesystem operation journal (completed);
4. PR #9, stopped-directory atomic publication (completed);
5. PR #10, same-process stopped-writer capability (completed);
6. PR #11, stopped-directory backend conformance (completed);
7. PR #12, authenticated committed-result reconciliation (completed);
8. same-image resume verification and rollout-tail repair;
9. ext4 or filesystem-image physical backend; and
10. differential export.

This order keeps orchestration semantics testable before selecting a physical
format, and requires same-image Codex recovery evidence before optimising
transport or retention.
