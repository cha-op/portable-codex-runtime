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

`captureCleanCheckpoint()` and `restoreCleanCheckpoint()` are the orchestration
entry points. Both reject `graceful-abort` and `crash-prefix` before backend
dispatch. Successful results contain the deeply frozen validated checkpoint
descriptor and exact storage mutation result; neither result gains new
authority fields.

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

Before returning success, a concrete backend must:

- atomically recheck the complete canonical writer fence against the mutation;
- bind the operation ID to the exact session, storage, checkpoint or artefact
  target, complete checkpoint descriptor, and writer tuple;
- for capture, bind that operation to the exact attachment ID and attachment
  proof supplied by the validated attachment snapshot, authenticate the
  stopped-writer evidence handle, and bind it to the same transaction;
- persist and replay an exact idempotent result without repeating the physical
  mutation, including the exact checkpoint descriptor echo;
- prove that a restore destination is detached and isolated before mutation;
- establish the required storage barrier for clean capture; and
- perform the physical capture or restore and return its durable proof.

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

This first core deliberately exposes no replay-only reconciliation entry point.
Calling capture or restore again still performs the normal current-fence and
lease-expiration checks before dispatch, so it cannot reconcile a completed
operation after authority has expired or moved. A later API must query or
replay an exact durable result under the same operation ID without executing a
new mutation. Until then, uncertainty remains non-retryable at this layer and
requires backend-specific operator reconciliation.

## Explicitly Deferred Work

This core does not yet provide:

- evidence for a graceful `turn/interrupt` abort boundary;
- atomic `crash-prefix` capture or rollout-tail repair;
- replay-only reconciliation after an uncertain result and fence turnover;
- a stopped-directory backend and its conformance suite;
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
5. PR #10, same-process stopped-writer capability;
6. PR #11, stopped-directory backend conformance;
7. replay-only uncertain-result reconciliation, followed by same-image resume
   verification and rollout-tail repair;
8. ext4 or filesystem-image physical backend; and
9. differential export.

This order keeps orchestration semantics testable before selecting a physical
format, and requires same-image Codex recovery evidence before optimising
transport or retention.
