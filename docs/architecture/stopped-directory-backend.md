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

- `STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION`, with value `1`;
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

Only `captureCheckpoint` and `restoreCheckpoint` are implemented by this
adapter.

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

The mutation authority exposes exact own-data `runCapture` and `runRestore`
methods. It is the durable catalogue and admission seam: before invoking the
backend callback, it must reserve the exact request, checkpoint descriptor,
and predetermined result. During the complete callback it must hold the
canonical fence and attachment/launcher admission guard. After a successful
callback it must durably and idempotently finalize the catalogue or restore
destination and return the exact same completion object.

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
The versioned publication binding contains the reservation ID, exact
checkpoint, attachment ID, attachment operation and proof, and the
process/writer/stop correlation IDs. It contains no capability, writer handle,
absolute path, or credential.

The frozen completion contains `artifactProof`, `materialization`,
`replayed`, and `result`. A successful normal capture always has
`replayed: false`. Any pre-existing `prepared`, `materialized`, or `committed`
journal phase conflicts with fresh preparation and becomes terminal backend
uncertainty; serialized reservation and process/writer/stop correlation IDs
are not proof that an earlier artifact was created after the current writer
stop. Exact replay belongs to a later authenticated reconciliation API, not a
new stopped-writer capability.

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

Both mutation-authority methods must await their single callback. Calling it
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
replacement, speculative cleanup, or reconciliation. Retained staging,
published objects, and durable reservations remain evidence for an operator or
the later replay-only reconciliation path.

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

- replay-only reconciliation after an uncertain result or fence turnover;
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
