# Atomic Crash-Capture Extension

## Status and Scope

This document defines the dormant, provider-neutral version 1 extension for
capturing one `crash-prefix` checkpoint at an atomic storage boundary. The
extension gives a future authority a closed request, result, and committed
verification vocabulary. A separate durable PostgreSQL catalogue and classic
LVM snapshot provider implement that private boundary. The session authority
also has a durable force-fence-to-capture handoff foundation, but it stops at a
prepared capture blocker: no currently assembled runtime or public deployment
can take the checkpoint through that branch.

The extension is separate from the base storage backend contract. A backend
opts in through `captureAtomicCrashCheckpoint()` and
`verifyCommittedAtomicCrashCheckpoint()`; ordinary clean checkpoint capture
does not discover or call either method. The narrow facade captures the
extension methods and identity for same-process use, while the crash-capture
core issues a separate one-use preparation token for each validated request.
Neither action is durable admission, a physical fence, or proof that a
provider performed an atomic capture.

In particular, the Linux ext4 backend continues to advertise:

```js
{
  atomicPointInTimeCheckpoint: false,
  exclusiveWriterAttachment: true,
  fencing: "manual",
  normalDirectoryAttachment: true,
}
```

Nothing in this contract changes those declarations or connects ext4 to the
extension.

## Version 1 Records

All records are exact, deeply frozen, secret-free data. Unknown fields are
rejected. Identifiers and tuple fields are validated through the existing
storage records rather than accepted as unrelated provider metadata.

### Capture request

The capture request has this exact outer shape:

```js
{
  captureAttemptId,
  checkpoint,
  contractVersion: 1,
  mutationRequest,
  sourceAttachment,
  storageRef,
}
```

- `captureAttemptId` identifies this one logical physical attempt.
- `checkpoint` is the predetermined version 1 descriptor. It must be a
  `crash-prefix` checkpoint and names the exact checkpoint and artifact IDs to
  publish.
- `mutationRequest` is the existing version 1 `checkpoint` mutation request.
  It binds the operation ID and target IDs to the source backend, session,
  storage, holder, lease, and fencing epoch.
- `sourceAttachment` is the exact existing read-write directory attachment
  whose writer boundary a future composition must authenticate as completely
  stopped or physically fenced. Its backend, session, storage, holder, lease,
  epoch, and attachment identity must match the request tuple.
- `storageRef` fixes the backend, session, and storage identity independently
  of the host-local attachment path.

Cross-field validation makes the outer record one closed request. A caller
cannot substitute another checkpoint, artifact, operation, source attachment,
storage object, session, or fencing epoch while retaining the same attempt ID.
Validation is structural binding only: it cannot establish that the named
writer is physically unable to mutate storage.

The request deliberately contains no self-authenticating physical-authority
field. A future composition must obtain and authenticate the opaque
stopped-or-fenced writer authority at dispatch, and the concrete provider must
revalidate that authority's binding before performing the atomic capture.

### Committed result

A successful result has this exact outer shape:

```js
{
  artifact: {
    byteLength,
    contentSha256,
    objectId,
    objectIdentityScheme,
    readOnly: true,
  },
  artifactId,
  backendId,
  captureAttemptId,
  checkpointId,
  contractVersion: 1,
  operationId,
  proofId,
  sessionId,
  sourceFencingEpoch,
  status: "committed",
  storageId,
}
```

The result echoes the durable capture identity selected from the request. The
backend, session, storage, checkpoint, artifact, operation, attempt, and source
fencing epoch must all match. The original request remains authoritative for
the source attachment, holder, and lease binding; the result is accepted only
when validated against that request. `proofId` is an opaque provider receipt
identifier, not an independently sufficient safety proof. The `artifact`
record is the committed observation that a future provider catalogue must
persist and later revalidate. `byteLength` is a canonical positive uint64
decimal string, and `contentSha256` is exactly 64 lowercase hexadecimal digits
without a `sha256:` prefix. An exact retry may replay only the same complete
result, including the proof and all five artifact fields.

### Committed verification

Source-free verification returns exactly one of these records:

```js
{
  contractVersion: 1,
  outcome: "committed",
  result: committedResult,
}
```

```js
{
  contractVersion: 1,
  outcome: "unknown",
  result: null,
}
```

The verifier receives the immutable capture request and performs read-only
verification against provider-committed state. The request retains the
original `sourceAttachment` record for identity binding, but verification must
not resolve or reopen its `rootPath`, require live source authority, or perform
a fresh capture. A `committed` outcome is admissible only when the exact
committed result can be reconstructed and validated against the original
request.

`unknown` means that committed success could not be proved. It is not proof of
absence, rollback, quiescence, or permission to retry the provider mutation.
Timeouts, unreadable state, ambiguous acknowledgements, missing provider
authority, and conflicting evidence all remain non-authorizing through this
outcome. Version 1 intentionally has no `absent` outcome.

## Same-Process Preparation

The extension facade is process-local. It validates one concrete backend
object, captures the base contract version, backend ID, capabilities, and
required operations only through a non-proxy data-property chain, and requires
the three extension fields to be own data properties. Accessors and a proxy at
any inspected prototype depth are rejected without being invoked. The facade
uses the captured identity, capability snapshot, and extension methods rather
than rereading the backend after validation. Reapplying
`createAtomicCrashCaptureBackendFacade()` to that facade in the same process
returns the same object and does not look the methods up again.

`prepareAtomicCrashCapture()` separately binds one normalized request to one
frozen, object-identity token. `capturePreparedAtomicCrashCheckpoint()`
consumes that token synchronously before invoking the provider, so provider
rejection or acknowledgement loss cannot reopen it. A second dispatch through
the same token is rejected.

These rules prevent a mutable backend object, prototype replacement, accessor,
or method swap from changing the implementation held by one facade, and bind
one token to one dispatch attempt. They do not survive a process restart and
create no provider catalogue entry by themselves. The process-local brands are
not a global registration of the underlying implementation, and independently
creating a new facade or token is not a durable deduplication mechanism. The
private LVM provider therefore obtains its dispatch decision from the durable
catalogue before it presents stopped-writer authority or makes any
state-changing LVM call. Before the claim, it permits only the bounded,
read-only LVM observations described below to resolve and canonicalize the
provider binding.

## Durable Catalogue and Classic LVM Provider

Migration 12 adds an independent `atomic_crash_captures` catalogue. Attempt,
operation, checkpoint, and artifact IDs are stored in four separately unique
opaque columns, with no lifecycle foreign key or UUID alias. Each row binds the
canonical request and provider binding by exact JSON plus SHA-256 and moves
only through these irreversible states:

```text
starting -> uncertain
starting -> committed
uncertain -> committed
```

The database owns `claimed_at`, `uncertain_at`, and `committed_at` transaction
timestamps and rejects identity, request, binding, result, or timestamp
replacement as well as delete and truncate. Only a newly and unambiguously
committed `starting` insert returns one process-local dispatch claim. An
existing `starting` or `uncertain` row, or an insert whose commit acknowledgement
was lost, returns no claim. A committed row can replay only its exact validated
provider binding and result. The dispatch claim is consumed before either
`uncertain` or `committed` is written, so a malformed result, database error, or
acknowledgement loss cannot reopen physical dispatch.

Provider-binding admission measures the exact canonical JSON text that the
application hashes and persists, encoded as UTF-8, rather than PostgreSQL's
version-dependent binary `jsonb` representation. The database bounds that same
text at 65,536 bytes, requires its parsed JSONB value to match the retained
binding, and keeps both forms immutable across every state transition. The
application rejects NUL and unpaired-surrogate strings or keys before database
access because PostgreSQL JSONB cannot represent that input domain.

The dormant LVM wrapper delegates all seven base lifecycle methods and changes
only its private capability snapshot to
`atomicPointInTimeCheckpoint: true`. Before claiming, its injected origin
resolver supplies a stable origin LV UUID and requested COW byte count. The
driver reads that origin's VG extent size, rounds the request upward without
crossing uint64, and derives the deterministic classic-snapshot binding from
the canonical COW allocation. After the claim, an injected authority consumer
must authenticate the exact opaque stopped-writer authority and request in the
same callback that performs the one permitted `lvcreate`.

The retained artifact is the read-only snapshot LV itself. Its persistent
object identity is the snapshot LV UUID. Content stability is the independently
observed block-device byte length plus a full streaming SHA-256, and access
policy requires both read-only LVM attributes and a read-only block device.
The snapshot name, tag, origin UUID, active-valid classic-snapshot state, COW
usage below 100 percent, device-mapper UUID, and a same-observation-window
major/minor pair provide provider and attachment checks. COW allocation is not
the snapshot's visible byte length: the VG extent size canonicalizes the former
before durable admission, LVM must report that exact allocation for the
snapshot LV, and the read-only block device exposes the origin-sized latter
while a complete stream confirms that artifact length.

Committed replay physically revalidates the retained LV before returning its
result. Source-free verification reads only the catalogue and retained
snapshot evidence; it never resolves or opens `sourceAttachment.rootPath`,
reconstructs authority, or calls `lvcreate`. Missing, unreadable, writable,
exhausted, replaced, or content-mismatched evidence returns `unknown` and never
authorizes a second dispatch.

## Protected Properties

The artifact signals protect three distinct properties. Implementations must
not collapse them into a generic metadata or `stat` comparison.

### Object identity

Object identity is the pair
`(objectIdentityScheme, objectId)`. The scheme defines how the provider names
objects, while the opaque object ID selects one object within that scheme.
Both fields are required because an ID from one namespace cannot safely be
compared with an ID from another. A change to either field is object
replacement, even when the pathname, size, or digest is unchanged.

### Content stability

Content stability is the pair `(byteLength, contentSha256)`. `byteLength`
prevents accepting a different byte extent and supplies an exact bounded
length for verification. `contentSha256` binds every byte in that extent and
detects same-length content mutation. Neither field establishes object
identity: a replacement object may contain identical bytes.

### Access policy

Access policy is represented by `readOnly`, and version 1 accepts only
`readOnly: true`. This signal states the minimum policy required of a committed
capture artifact: ordinary writer access must not be able to mutate it. An
unchanged object and digest are insufficient if the artifact has become
writable. Failure to inspect or revalidate the policy is uncertainty, not a
read-only proof.

Child-entry churn, timestamp changes, directory size or link-count changes,
mount bookkeeping, cache state, and File Provider-style materialization may
change benign metadata without changing any of these protected properties.
They must not be reported as object replacement, content mutation, or an
access-policy change unless the corresponding selected signal also changes.
Conversely, a mismatch in a selected signal must not be excused merely because
other metadata appears stable.

## Private Complete-Stop LVM Composition

The PostgreSQL launcher now keeps a separately branded module-private facet for
an exact version 1 atomic request. The existing nine-method clean launcher
facade, its `clean` tuple admission, and its stop-operation identity remain unchanged.
The private facet instead derives a domain-separated stop operation from the
launch attempt and the complete atomic request, including the capture-attempt
ID. It retains the opaque stopped-writer capability and its attachment, lease,
holder, epoch, process, writer, and stop-operation bindings inside the launcher.
Neither the raw facet nor an accessor for its complete, authority-consume, or
retirement methods is exported. The ordinary launcher factory and detached
runtime composition receive only the unchanged nine-method facade. A separate
launcher-owner factory returns that facade together with one fresh, frozen,
null-prototype, zero-own-key `atomicCrashCaptureAssembler` identity capability.
The launcher module brands the capability in a private `WeakMap`; it is not an
own property of the launcher and cannot be recovered from, cloned from, or
structurally imitated by an ordinary facade holder.

The public composition factory accepts only that owner-held assembler
capability before binding the selected backend, catalogue, and driver. It
resolves the matching facet only inside the launcher module and returns solely
`runCapture` and `reconcileCapture`. The assembler is intentionally a
transferable bearer capability and must remain with the authorized assembly
root. Possession of the ordinary launcher facade, a cloned empty object, or an
assembler for a different launcher cannot release this launcher's blocker with
caller-constructed collaborators or results.

The private PostgreSQL/LVM assembler constructs the LVM provider with that
facet's authority consumer before any writer authority is presented. A fresh
capture therefore preserves this order:

1. validate and freeze the exact provider backend and atomic request;
2. establish the matching durable complete-stop and issue one same-process
   stopped-writer capability;
3. let the catalogue grant a fresh provider dispatch;
4. consume that exact capability while the driver performs one snapshot;
5. commit and validate the exact version 1 result; and
6. retire the local stopped-writer blocker only after committed evidence.

A committed catalogue replay physically revalidates the retained snapshot
without consuming authority. The facet then revokes the unused stopped-writer
capability before retirement. If provider acknowledgement is ambiguous, the
composition performs only source-free committed verification. `unknown`, a
verification error, stop ambiguity, or authority-consumption ambiguity keeps
the local blocker and cannot reopen preparation, stop, capability consumption,
or physical dispatch. A later same-process reconciliation is admitted only for
that retained exact request and repeats only committed verification.

An exact `runCapture` retry after `completeStop` rejects is different: the
composition retains the pre-provider attempt in `stop-uncertain` and re-enters
the launcher's same durable stop operation. A transient failure before any
physical stop can therefore recover, while a launcher record that reached an
ambiguous or non-replayable state still rejects without admitting the provider
or a second physical stop. After exact retirement, the composition replaces
the mutable authority attempt with a frozen request/result terminal record.
Exact `runCapture` or `reconcileCapture` response-loss retries return that same
result without another stop, catalogue claim, authority consume, or snapshot;
reuse of the capture-attempt ID with different request content remains closed.

## Durable Physical-Fence Handoff Foundation

Writer force-fence request version 2 adds one complete
`atomicCapture: { operationId, request }` intent for an
`atomic-crash-capture-v1` operation. The
`writer-fence-atomic-capture-intent-v2` claim permanently preclaims that
capture operation ID before force-fence dispatch. It binds the exact session
and immutable atomic request independently of the force-fence operation ID.
Admission also binds the checkpoint's Codex session, root thread, and image
digest to the session manifest. Another operation kind, session, manifest,
request, or capture attempt therefore cannot adopt the identity after an
external fence effect.

`finalizeWriterForceFenceAtomicCaptureHandoff()` accepts only the exact trusted
provider result for the dispatched version 2 request. In one serializable
transaction it:

1. validates the opaque physical-fence proof against the revoked attachment,
   lease, holder, old epoch, target, storage, and force-fence operation;
2. commits the force-fence operation and enters `DETACHED`;
3. materializes the preclaimed atomic-capture operation and its exact request
   in `prepared`; and
4. makes that capture operation the session's active reservation.

Migration 013 also checks this reverse relation at commit time: a successful
V2 fence cannot commit unless the exact prepared capture operation and
reservation, released fence reservation, and `DETACHED` session terminal and
active pointers all agree. The fence identity and terminal row and the
prepared capture row are immutable in this foundation, while deferred checks
revalidate the relation after claim, reservation, or session changes. A direct
SQL writer subject to these triggers therefore cannot later hide the fence or
release or detach its blocker. Blocked or pre-dispatch-cancelled fences leave
the preclaim unmaterialized and do not create a capture blocker.

Readback reconstructs the exact fence terminal document and substitutes only
the prepared capture active pointer. The current document version and complete
fence `lastOperation` pointer must still match, preventing a structurally valid
but older or crossed session document from being accepted as the handoff.

The `DETACHED` lifecycle therefore does not mean that a successor writer is
admissible. The active prepared capture remains a database-authoritative,
session-conflicting blocker across process exit, acknowledgement loss, and
restart. Exact finalization replay and dedicated handoff readback recover only
the same committed fence result plus the same prepared capture;
`reconcileWriterForceFenceAtomicCaptureHandoff()` is that restart-safe read
boundary. Neither path invokes a fence provider nor creates, replaces,
dispatches, or retires a capture.

This foundation keeps physical exclusion evidence separate from logical
ordering. A database epoch advance, lease expiry, `FENCING`, `BLOCKED`, or
`DETACHED` state is not an authentic physical-fence proof. Only the exact
provider result accepted by the force-fence finalizer may materialize the
handoff. Conversely, that proof does not prove that an atomic snapshot exists:
the separately prepared capture must still be dispatched through a concrete
provider and committed before any later repair or writer admission.

This slice deliberately exposes no release path for the prepared blocker. It
does not dispatch the LVM provider, reconcile a snapshot, repair a tail,
restore a writable generation, or admit a higher-epoch writer. Until those
later transitions exist, an exact committed handoff remains fail-closed and
the session stays blocked from successor admission.

## Deliberate Non-Capabilities

The complete-stop composition and durable physical-fence handoff foundation
still do not provide or select:

- a concrete automatic stale-writer fence provider or real provider evidence;
  version 2 authenticates a proof returned by a separately trusted provider,
  while the complete-stop path accepts only its conclusively joined local
  writer stop;
- dispatch or reconciliation of the prepared physical-fence-bound capture, or
  release of that durable blocker;
- integration with the current clean checkpoint path, lifecycle facade,
  ext4 capability discovery, or public deployment;
- crash-prefix tail repair;
- restore or publication of a writable restore destination; or
- admission of a new writer lease or fencing epoch.

The private composition closes only the local complete-stop branch. The
durable version 2 handoff closes the restart-safe authority gap between an
authenticated physical fence and a prepared atomic capture, but it does not
perform either external effect. A future production recovery path must connect
a real fence provider and atomic provider to that authority before stale-writer
takeover is possible. Cloud, filesystem-native, thin-LVM, and other snapshot
adapters also remain separate work.

## Required Future Ordering

A production crash-prefix path must preserve this safety order. A conclusively
joined complete writer boundary may satisfy the first step for a local stopped
capture; automatic stale-writer takeover must finish the provider composition
around the durable physical-fence handoff:

1. establish and authenticate either complete writer stop or a physical fence
   for the old writer;
2. perform a stop-or-fence-bound atomic capture through a concrete provider and
   commit the exact version 1 result;
3. repair the captured tail only on a separate writable generation, never on
   the committed read-only artifact; and
4. admit a writer only for that repaired generation under a strictly higher
   fencing epoch and a new lease.

No later step may compensate for skipping or weakening an earlier one. In
particular, successful structural validation, an incomplete process exit,
lease expiry, `unknown` verification, or a read-only artifact observation
cannot stand in for the required complete-stop or physical-fence authority.
