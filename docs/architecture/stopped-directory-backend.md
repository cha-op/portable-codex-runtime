# Stopped-Directory Backend

## Scope

The stopped-directory backend is the first complete storage-backend adapter
for stopped-writer `clean` checkpoint capture and restore. It composes the
same-process stopped-writer coordinator, a durable mutation-authority seam,
the local stopped-directory publication layer, and the backend-neutral
snapshot core's exact result contract.

The adapter owns only checkpoint capture and restore. Session provisioning,
writable attachment preparation, detach, fencing, and destruction remain the
responsibility of a separately validated lifecycle backend. This division
keeps storage lifecycle policy out of the local publication primitive and
prevents the adapter from inventing attachment or lease authority.

The module exports:

- `STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION`, with value `2`;
- `StoppedDirectoryBackendError`; and
- `StoppedDirectoryBackend`.

The constructor accepts exactly:

```js
new StoppedDirectoryBackend({
  backendId,
  coordinator,
  lifecycleBackend,
  mutationAuthority,
  publication,
  resolveStoppedWriter,
});
```

The lifecycle backend must satisfy `assertStorageBackend()` and use the exact
same `backendId`. The resulting adapter also satisfies the v1 storage-backend
shape and advertises:

```js
{
  normalDirectoryAttachment: true,
  exclusiveWriterAttachment: true,
  fencing: "manual",
  atomicPointInTimeCheckpoint: false,
}
```

These capabilities describe a trusted local-directory development and
conformance backend. They do not claim automatic failover, a live
crash-consistent volume snapshot, or remote-filesystem fencing.

The adapter separately advertises
`captureReconciliationContractVersion: 1` and implements the optional
`reconcileCheckpointCapture()` extension. The extension does not change the v1
base storage-backend method set.

## Lifecycle Delegation

The adapter delegates these five operations to the validated lifecycle
backend:

- `provisionSession`;
- `prepareWritableAttachment`;
- `detachAttachment`;
- `forceFence`; and
- `destroySession`.

It does not cache, reinterpret, or replace the lifecycle backend's canonical
attachment and fencing decisions. In particular, declaring manual fencing does
not let the adapter promote a local lock, publication journal, or checkpoint
record into lease authority. The trusted lifecycle and mutation-authority
layers must prevent a stale writer from crossing the mutation guard.

`captureCheckpoint` and `restoreCheckpoint` implement the base mutation
contract. `reconcileCheckpointCapture` implements the optional committed
capture-reconciliation extension.

## Trusted Collaborators

The adapter closes over one designated stopped-writer coordinator. It never
constructs a replacement coordinator for a retry and never treats serialized
fields as a stopped-writer capability.

`resolveStoppedWriter({ attachment, checkpoint, request })` is synchronous.
It resolves trusted runtime state to exactly:

```js
{
  canonicalLeaseAtRegistration,
  processIncarnationId,
  stopOperationId,
  writer,
  writerIncarnationId,
}
```

An `AsyncFunction` resolver is rejected during backend construction. Bound and
native-code functions are also rejected because JavaScript does not expose a
bound target that can be safely classified without executing it or reading
spoofable metadata. Callers that need binding must supply an explicit
source-backed synchronous closure. If an ordinary resolver nevertheless
returns a same-realm native Promise, the backend observes that Promise only
through its safe local await boundary so a rejection cannot become
process-level unhandled state, then fails the capture as uncertain without
using the fulfillment value. The backend never invokes an unsafe thenable or
Promise constructor path; the resolver retains rejection observation
ownership for any unsafe asynchronous value it returns.

The supplied stopped-writer evidence remains the original process-local object
capability. The resolver record supplies the matching writer handle and
correlation values, but none of those fields independently carries stop
authority.

The mutation authority exposes exact own-data `runCapture`,
`runCaptureReconciliation`, and `runRestore` methods. It is the durable
catalogue and admission seam: before invoking a normal mutation callback, it
must reserve the exact request, checkpoint descriptor, and predetermined
result. During the complete normal callback it must hold the canonical fence
and attachment/launcher admission guard. After a successful callback it must
durably and idempotently finalize the catalogue or restore destination and
return the exact same completion object.

Before normal capture publication, `runCapture` must also atomically create an
authenticated durable capture-attempt record from absent state. Its opaque
attempt ID and complete v2 coordinator binding remain outside worker-controlled
storage as canonical authority data. `runCaptureReconciliation` must load that
same record by the original operation, serialize concurrent reconciliation,
and supply its exact binding, request, and predetermined result to the callback.
A journal record or caller-supplied attempt ID cannot substitute for that
lookup.

The canonical attempt passed across the seam has this exact data shape:

```js
{
  contractVersion: 1,
  captureAttemptId,
  operationId,
  state: "authorized" | "committed",
  binding: {
    contractVersion: 2,
    captureAttemptId,
    reservationId,
    checkpoint,
    attachmentId,
    attachmentOperationId,
    attachmentProofId,
    processIncarnationId,
    writerIncarnationId,
    stopOperationId,
  },
  request,
  result,
}
```

`authorized` means the attempt exists durably and may have a physically
committed publication whose catalogue finalization acknowledgement was lost.
`committed` means the authority has also finalized the exact catalogue result.
Transport-only `replayed` is not durable attempt authority; the canonical
catalogue stores the verified artefact proof and materialisation separately.

This repository defines and tests that seam. A production database, catalogue,
lease service, and launcher-admission implementation remain separate work.

## Capture Transaction

Capture follows one authority chain:

1. Validate deterministic request shape before invoking the resolver,
   coordinator, mutation authority, publication layer, or journal.
2. Resolve the exact stopped writer synchronously.
3. Consume the supplied capability through the designated coordinator with the
   resolved writer, process/writer incarnations, stop operation, registered
   canonical lease, and attachment.
4. Inside the coordinator's one `runSnapshot` callback, invoke
   `mutationAuthority.runCapture(admission, publish)`.
5. Let the authority reserve the exact request, descriptor, and predetermined
   `{ checkpoint, mutation }` result before it invokes `publish`.
6. Validate the complete authority context while its fence and admission guard
   is still held, then call `publishFreshCheckpointArtifact()`. Its atomic
   journal preparation accepts only an `absent` operation ID.
7. Return the publication completion to the authority, which must durably
   finalize the catalogue and return that same object before backend success.

The frozen capture admission contains the exact attachment, checkpoint,
request, process and writer incarnation IDs, and stop operation ID. It does
not contain the writer handle or the stopped-writer capability.

The authority invokes `publish(context)` exactly once with:

```js
{
  artifactDirectory,
  artifactOwnedRoot,
  captureAttemptId,
  canonicalAttachment,
  canonicalLease,
  now,
  reservationId,
  result,
  sourceDirectory,
  sourceOwnedRoot,
  storageRef,
}
```

The callback revalidates the authority clock, exact fence, storage binding,
attachment, predetermined result, and complete path plan before publication.
The versioned publication binding contains the capture-attempt and reservation
IDs, exact checkpoint, attachment ID, attachment operation and proof, and the
process/writer/stop correlation IDs. It contains no capability, writer handle,
absolute path, or credential.

The frozen completion contains `artifactProof`, `materialization`,
`replayed`, and `result`. A successful normal capture always has
`replayed: false`. Any pre-existing `prepared`, `materialized`, or `committed`
journal phase conflicts with fresh preparation and becomes terminal backend
uncertainty; serialized attempt, reservation, and process/writer/stop
correlation IDs are not independently proof that an earlier artifact was
created after the current writer stop. Exact replay belongs to the separate
authenticated reconciliation API, not a new stopped-writer capability.

## Committed Capture Reconciliation

`reconcileCheckpointCapture({ checkpoint, request })` receives only the exact
original clean-capture descriptor and mutation request. It does not resolve a
writer, consult or consume the same-process coordinator, accept a capability,
or require the old lease to remain current. The public core can therefore call
it after lease expiry or fence turnover without pretending that serialized
source-fence fields are present authority.

The backend invokes
`mutationAuthority.runCaptureReconciliation(admission, verify)`. The frozen
admission contains only `{ checkpoint, request }`. Before invoking `verify`,
the trusted authority must load the canonical durable attempt and pass an exact
context containing the artefact path plan plus that attempt's v2 binding,
request, and predetermined result. The backend validates every field, including
the opaque capture-attempt ID, and requires the attempt request and result to
match the admission exactly.

The callback calls `verifyCommittedCheckpointArtifact()` with no source path.
That publication method can only read and validate the exact committed journal
record and final artefact; it cannot advance `prepared` or `materialized`
state. The backend requires `replayed: true`, derives the canonical artefact
proof from the verified materialisation, and returns the same frozen completion
shape as capture. The authority must idempotently finalize or confirm the
catalogue and return the exact same completion object before success.

This division is the authentication boundary: the mutation authority proves
that the attempt record is canonical, while publication proves that its exact
binding and bytes are durably committed. Neither side alone is sufficient.
Legacy v1 journal bindings without capture-attempt provenance cannot pass this
path and are not automatically upgraded.

## Restore Transaction

Restore invokes `mutationAuthority.runRestore(admission, publish)`. Its
frozen admission contains the exact checkpoint and request. The authority
must reserve their predetermined result and hold a newer canonical writer
fence plus destination admission guard while it invokes the callback.
Before authority admission, the backend deterministically rejects a request
whose fencing epoch is not strictly newer than the checkpoint source epoch.
The authority must still revalidate the actual current canonical fence inside
its guarded transaction.

The authority invokes `publish(context)` exactly once with:

```js
{
  artifactDirectory,
  artifactOwnedRoot,
  artifactProof,
  canonicalLease,
  destinationDirectory,
  destinationIsolationProofId,
  destinationOwnedRoot,
  destinationState,
  now,
  reservationId,
  result,
  storageRef,
}
```

`destinationState` must be exactly `"detached"`. The callback revalidates
the newer current fence, storage binding, predetermined result, trusted
artefact proof, destination isolation proof, detached state, and complete path
plan before calling `publishRestoreDestination()`.

The frozen restore completion contains `materialization`, `replayed`, and
`result`. The authority must durably finalize launcher-visible destination
state and return that same completion object before the backend reports
success. A published path or journal record alone is not writable-launch
authority.

## Callback and Uncertainty Contract

All three mutation-authority methods must await their single callback. Calling it
zero times, more than once, after the authority method has returned, or
returning a substituted completion object fails closed as uncertain. The
authority guard must span the callback's complete asynchronous lifetime and
durable finalization; fence, attach, or launcher admission cannot cross it.
The backend attaches a local rejection observer to every native callback
Promise before returning that same Promise unchanged to the authority. This
contains ignored or late callback rejections without changing the identity or
settlement observed by an authority that correctly awaits the callback.

Each authority method must return a native Promise whose prototype-chain
`constructor` resolves through an own data property to the backend realm's
captured `Promise` constructor. Thenables, generator objects, Proxies,
constructor accessors, Promise subclasses, and cross-realm Promises fail closed
before the backend awaits them. This prevents Promise assimilation from
invoking attacker-controlled `then` logic in place of durable finalization.
An authority that uses a foreign or subclass Promise must await it inside its
own local `async` boundary and return that boundary's native Promise. Because
the backend cannot safely attach a rejection handler to a rejected value that
fails this preflight, the authority retains rejection-observation ownership for
every unsafe value it attempts to return.

Deterministic request-shape failures occur before any resolver, authority,
coordinator, publication, or journal dispatch. They use the fixed
`invalid_stopped_directory_backend_request` code.

After runtime collaboration can have started, failures collapse to the fixed
`stopped_directory_backend_outcome_uncertain` code. Public
`StoppedDirectoryBackendError` instances are frozen, non-retryable, and
path-free. They do not expose collaborator exceptions, absolute paths,
credentials, attachment details, or private authority state.

A capture failure after entering the coordinator callback is terminal for the
one-use capability. The backend performs no internal retry, coordinator
replacement, or speculative cleanup. Retained staging, published objects, and
durable reservations remain evidence. A caller may separately invoke committed
reconciliation under the durable attempt authority; that call never reuses the
terminal capability.

In particular, the normal capture path never adopts or advances an earlier
publication phase. The fresh journal transition is atomic with respect to the
canonical journal lock, so a record inserted after the publication preflight
also fails closed before source materialisation. Restore remains idempotently
replayable under its newer destination fence and trusted artifact proof.

## Storage and Fencing Boundary

The composed backend proves the local stopped-directory path only when:

- the same-process capability authenticates one fully joined writer stop;
- the mutation authority atomically rechecks and guards the canonical fence;
- the publication layer accepts the approved local-filesystem profile and
  completes its barrier, staging, atomic rename, sync, readback, and journal
  protocol; and
- the authority durably finalizes the exact predetermined result before
  success.

Its `fencing: "manual"` declaration means automatic partition recovery and
cross-host failover are not supported. Its
`atomicPointInTimeCheckpoint: false` declaration means it cannot emit a live
`crash-prefix` checkpoint. Rootless workers continue to see only an ordinary
directory; trusted host-side collaborators own lifecycle, fencing, and
publication authority.

NFS, SMB, distributed filesystems, object-store mounts, and unknown filesystem
semantics are outside this adapter's guarantee. Moving the same interface onto
shared storage requires a separate backend with server-side exclusive-writer
fencing, idempotency, publication, and fault evidence.

## Deferred Work

This backend deliberately does not provide:

- automatic repair or continuation of `prepared` or `materialized` capture
  attempts;
- a production linearizable lease, reservation, catalogue, and
  launcher-admission database;
- NFS or another remote/shared-filesystem adapter;
- an ext4 or filesystem-image physical backend and automatic host fencing;
- `graceful-abort` or live `crash-prefix` capture;
- same-image Codex resume verification or rollout-tail repair;
- differential compression, content-addressed storage, encryption, retention,
  periodic long-goal checkpoints, or cross-host migration verification; or
- the read-only Git Summary.

Git state remains optional user context and is not part of checkpoint
correctness, catalogue authority, or restore admission.
