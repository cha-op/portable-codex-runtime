# Same-Process Stopped-Writer Capability

## Scope

The stopped-writer capability coordinator turns one trusted, successful writer
shutdown into one in-memory authorization for one snapshot callback. It closes
the gap between a runtime supervisor that owns the writer lifecycle and the
backend-neutral snapshot core, which deliberately treats
`stoppedWriterEvidence` as opaque.

This capability is process-local authority. Its identity is authenticated by a
private `WeakMap`, not by fields that can be copied, serialized, signed, or
persisted. The coordinator binds the capability to:

- the exact issuing process incarnation;
- the exact writer incarnation within that process;
- the complete validated attachment snapshot;
- the writer fence tuple: session ID, lease ID, holder ID, and fencing epoch;
  and
- the exact trusted stop operation.

The process and writer incarnation IDs and the stop operation ID are
non-reusable correlation values created by the trusted runtime. They are useful
for exact binding and diagnostics but do not carry authority by themselves.
PIDs, process start times, thread IDs, Codex session IDs, rollout paths, and
installation IDs are not substitutes for either incarnation.

The coordinator does not implement a canonical lease database, storage
barrier, publication, durable idempotency, or cross-process recovery. PR #11
composes this capability with the stopped-directory publication layer and an
atomic canonical fence recheck.

## Trusted Stop Boundary

The stop callback is a trusted launcher or supervisor operation captured when
the writer incarnation is registered. It is not supplied by an untrusted
snapshot request and cannot be replaced after registration. The callback must
resolve with the exact exported `STOPPED_WRITER_STOP_CONFIRMED` sentinel only
after every process and task that can write the attachment has joined or is
conclusively unable to write. Generator and async-generator callbacks are
rejected; any other callback result is terminal stop uncertainty and cannot
mint authority.

Current Codex protocol events are useful lifecycle observations, but none is a
writer-stop proof:

- `turn/completed` and a stable interrupt marker show that a turn reached a
  terminal boundary; the loaded thread can still write later;
- `ShutdownComplete` does not prove that every persistence shutdown succeeded;
  and
- `thread/closed` does not prove that every rollout writer task and file handle
  has joined.

`thread/unsubscribe`, rollout flush, and absence of an active turn are likewise
insufficient. A clean checkpoint may use those facts as prerequisites, but the
coordinator must not mint authority from them.

Production issuance therefore requires one of these stronger boundaries:

1. the trusted supervisor stops and joins the complete container, cgroup, or
   VM writer incarnation; or
2. a future Codex shutdown API propagates writer failures and joins every
   persistence writer before reporting success.

A host process group alone is not complete containment because a descendant
can detach into another group. A process-kill boundary can prove that the
contained process incarnation no longer writes, but it is a `clean` checkpoint
boundary only when the required clean storage and application barriers were
also established.

## Authority Objects

Both the writer handle and the stopped-writer capability are frozen opaque
objects. Authority authentication uses coordinator-private `WeakMap` records;
private lifecycle and attachment-slot state also keeps an active writer record
strongly referenced in a `Map` so losing a handle fails closed instead of
silently releasing the slot. The lifecycle owner must retain the handles needed
for explicit revocation and retirement. Public properties, a random token
string, a symbol brand, or a serialized envelope are not accepted as authority.
Slot identity is stored in nested `Map` instances keyed directly by the validated
session, backend, and storage ID strings. It never serializes a composite object,
so inherited `toJSON` hooks or other prototype mutations cannot change a slot
lookup or admit a second writer.

JSON, `structuredClone`, a worker message, object spread, or any other copying
mechanism can at most create an inert lookalike. Only the exact original object
identity is accepted by the issuing coordinator. A capability from another
coordinator or writer incarnation is invalid even when every portable data
field is byte-for-byte equal.

The trusted runtime must construct exactly one designated coordinator for a
finite live issuer scope, retain it for every writer lifecycle in that scope,
and close the backend over that exact instance. A scope should normally be one
session/storage lineage, or another explicitly bounded set of slots; one
process-wide coordinator must not absorb an unbounded stream of ephemeral
session or storage IDs. The one-stop, one-capability guarantees are scoped to
this designated issuer. Another coordinator has independent private state and
cannot authenticate authority at the designated backend; creating one as a
retry mechanism is a trusted-runtime misconfiguration, not a recovery
transition.

The complete attachment and lease are copied first into exact, flat, frozen
records with coordinator-captured intrinsics. The storage contract validators
receive those private records only for pass/fail validation; their returned
objects are never retained as authority. Consumption compares the private
snapshot, not a caller-mutated object; poisoned clone or freeze operations
inside a shared validator therefore cannot substitute a caller-owned authority
record. The binding includes the attachment ID, proof ID, operation ID, root
path, storage and backend IDs, session and fence fields, kind, and mode.
Comparing only the attachment ID would permit detach, reattach, or
identifier-reuse ABA.

The fence binding uses the immutable writer identity tuple rather than lease
expiration. A lease renewal may extend `expiresAt` without creating a new
writer incarnation. PR #11 must still use the trusted authority clock and
atomically confirm that the canonical lease is current when it performs a new
physical mutation.

## Coordinator API

The public surface is deliberately narrow:

- register one running writer incarnation with its process incarnation,
  complete attachment, fence tuple, and trusted stop callback;
- invoke that stop operation exactly once and issue exactly one capability;
- consume that capability exactly once around one snapshot callback; and
- revoke the writer or its unconsumed capability, then retire a safely stopped
  terminal writer before a higher-fence incarnation can occupy its slot; and
- dispose the finite issuer only after every writer in it has retired.

Conceptually, the lifecycle is:

```js
async function stopWriter(binding) {
  await supervisor.stopAndJoinWriter(binding);
  return STOPPED_WRITER_STOP_CONFIRMED;
}

const writer = coordinator.registerWriter({
  attachment,
  canonicalLease,
  processIncarnationId,
  stopWriter,
  writerIncarnationId,
});

const stoppedWriterEvidence = await coordinator.stopAndIssueCapability({
  processIncarnationId,
  stopOperationId,
  writer,
  writerIncarnationId,
});

const result = await coordinator.consumeCapability({
  attachment,
  canonicalLease,
  capability: stoppedWriterEvidence,
  processIncarnationId,
  runSnapshot,
  stopOperationId,
  writer,
  writerIncarnationId,
});
```

`revokeWriter()` invalidates a running, stopping, issued, or stop-uncertain
writer; during `CONSUMING` it records a revocation request so the callback can
settle but cannot report successful authority. `retireWriter()` releases the
attachment slot only after successful consumption, or after a successful stop
whose unconsumed capability was revoked. A failed or ambiguous stop cannot be
retired optimistically.

There is no public `verify()`, decoder, serializer, state setter, or constructor
that accepts a caller declaration such as `stopped: true`. Authentication and
consumption are one operation so a successful check cannot be separated from a
later callback by a restart or ABA window.

## State Machine

One writer incarnation follows this monotonic lifecycle:

```text
RUNNING -> STOPPING -> ISSUED -> CONSUMING -> CONSUMED
             |            |          |
             v            v          v
       STOP_UNCERTAIN   REVOKED   OUTCOME_UNCERTAIN
```

The transition to `STOPPING` occurs synchronously before invoking or awaiting
the trusted stop callback. Concurrent stop attempts do not invoke the callback
again or receive another capability. A callback rejection, hostile thrown
value, revocation, or ambiguous stop result is terminal for that writer
incarnation and produces no capability.

Consumption first snapshots all ordinary options, authenticates the exact
writer and capability identities, and verifies their private binding. It then
transitions synchronously from `ISSUED` to `CONSUMING` before invoking or
awaiting `runSnapshot`. No getter, proxy trap, callback, promise assimilation,
or other user-controlled code runs between authentication and that transition.

The callback executes while the writer remains stopped. A synchronous,
microtask, or asynchronous recursive consume sees `CONSUMING` and cannot invoke
another callback. The lifecycle owner must not start a replacement writer
until the callback settles and the old incarnation is terminal.

Callback success reaches `CONSUMED`. Callback failure, cancellation,
revocation after dispatch, an abnormal thenable, or a proxy or generator-object
result is terminal uncertainty: the callback may have changed storage, the
capability is never reusable, and the coordinator does not synthesize success.
Both the stop callback and snapshot callback are checked before their first
coordinator-owned `await`. The coordinator inspects each direct return without
invoking accessors. A non-Promise object is accepted only when every object
traversed before the nearest `then` descriptor is non-proxy and that descriptor,
if present, is a non-callable data descriptor. A branded Promise is accepted
only when its nearest `constructor` descriptor is a data descriptor containing
the module-captured Promise intrinsic. This keeps `await` on the exact-Promise
path, so it attaches a settlement observer without reading a hostile `then`
value. After the snapshot callback settles, the descriptor-only `then` check
runs again and prevents the async method return from performing a second,
stateful thenable assimilation after recording successful consumption.

Trusted stop and snapshot callbacks must normalize Promise subclasses,
cross-realm Promises, and Promises with accessor or foreign `constructor`
descriptors inside their own async boundary before returning. ECMAScript
provides no accessor-free way to attach a rejection observer to such a
contract-violating Promise. The coordinator therefore reports terminal
uncertainty without touching its hooks, while ownership of any underlying
rejection remains with the callback. A conforming callback's accepted Promise
may itself assimilate a thenable before the coordinator observes the
fulfillment value; that Promise chain remains inside the trusted backend
boundary.
Stop failure is likewise terminal because it may have partially quiesced the
writer.

Every actual writer start creates a new writer incarnation. Reusing a PID,
attachment record, thread ID, session ID, or fence tuple cannot revive an old
record or capability. `retireWriter()` releases a safely stopped terminal slot;
the replacement must carry a strictly higher fencing epoch, and all old
capabilities remain invalid forever.

An open coordinator deliberately retains each retired slot's last fencing
epoch so a stale replacement cannot erase that monotonic history. The lifecycle
owner must call terminal `dispose()` when the finite issuer scope ends and every
writer has safely retired. Disposal permanently closes that instance and drops
its slot, writer, and capability containers; every later entry point fails
before reading caller input or dispatching a callback. It does not authorize a
new coordinator for an unfinished mutation. A new issuer scope must first pass
the external canonical fence admission, and the owner must also drop any
backend closure over the disposed instance. A stop- or snapshot-uncertain scope
can never satisfy `dispose()` because its writer cannot safely retire. After
canonical fencing and scope teardown resolve outside this primitive, the owner
abandons that bounded coordinator without successful disposal and drops it
together with every backend closure and remaining handle.

## Hostile Inputs and Public Errors

Public option envelopes and portable binding data accept only exact plain data
properties. Proxies, revoked proxies, accessors, symbols, inherited authority,
unexpected fields, and missing fields fail before stop or snapshot dispatch.
Capability authentication performs no property reads on the presented object;
it uses only private object identity.

Public errors use fixed codes, messages, and path-free stack text, are frozen
and non-retryable, and contain no callback exception, filesystem path,
attachment metadata, credential, or prompt. A callback cannot forge an internal
error by throwing a lookalike public error. Once the stop or snapshot callback
may have run, the coordinator reports terminal uncertainty rather than exposing
collaborator details or retrying.

## Backend Composition

The snapshot core continues to pass the capability through unchanged. The
stopped-directory backend in PR #11 must close over the designated coordinator,
resolve the current writer incarnation from trusted runtime state, and call
`consumeCapability()` around the complete backend path. A new or resumable
mutation callback must include the atomic canonical fence recheck, exact
operation binding, storage barrier, publication, and durable result commit.
That backend must also preserve the finite issuer-scope ownership contract and
dispose its coordinator only after canonical lifecycle teardown and safe writer
retirement.

An exact committed-result replay performs no new physical mutation. Inside the
same one-use callback, PR #11 must verify both the exact committed journal
binding and the published final object's topology, persistent identity, and
modeled digest before returning the durable result. A journal record alone is
not replay authority.

The single current dispatch may resume an exact pre-existing `prepared` or
`materialized` operation. After callback failure or uncertainty, however, this
coordinator has no transition that mints another capability for the same writer
incarnation. The operation remains blocked for the later dedicated
reconciliation path; neither the old capability nor a newly instantiated
coordinator may be used to re-dispatch it.

The capability itself is intentionally absent from journal records,
checkpoint descriptors, manifests, snapshot archives, and control-plane
databases. Host migration requires the old process to finish or be fenced and
the new host to establish a new writer incarnation; it never transfers this
same-process authority.

## Deferred Work

This layer does not provide:

- canonical lease or attachment admission;
- stopped-directory backend conformance;
- replay-only reconciliation after uncertain outcomes;
- graceful-abort or live `crash-prefix` evidence;
- a joined Codex persistence-writer shutdown API;
- cross-process, cross-host, or restartable stop capabilities;
- physical storage fencing against an uncontained stale writer; or
- system-wide intrinsic hardening of shared storage-contract validators beyond
  this coordinator's independently owned authority snapshots.
