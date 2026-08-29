# Atomic Crash-Capture Extension

## Status and Scope

This document defines the dormant, provider-neutral version 1 extension for
capturing one `crash-prefix` checkpoint at an atomic storage boundary. The
extension gives a future authority a closed request, result, and committed
verification vocabulary. A separate durable PostgreSQL catalogue and classic
LVM snapshot provider now implement that private boundary, but no currently
assembled runtime or public deployment can take the checkpoint.

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
catalogue before it presents stopped-writer authority or calls LVM.

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

The dormant LVM wrapper delegates all seven base lifecycle methods and changes
only its private capability snapshot to
`atomicPointInTimeCheckpoint: true`. Before claiming, its injected origin
resolver derives a deterministic classic-snapshot binding from a stable origin
LV UUID and a COW allocation. After the claim, an injected authority consumer
must authenticate the exact opaque stopped-writer authority and request in the
same callback that performs the one permitted `lvcreate`.

The retained artifact is the read-only snapshot LV itself. Its persistent
object identity is the snapshot LV UUID. Content stability is the independently
observed block-device byte length plus a full streaming SHA-256, and access
policy requires both read-only LVM attributes and a read-only block device.
The snapshot name, tag, origin UUID, active-valid classic-snapshot state, COW
usage below 100 percent, device-mapper UUID, and a same-observation-window
major/minor pair provide provider and attachment checks. COW allocation is not
the snapshot's visible byte length: LVM reports the former for the snapshot LV
and the driver matches it to the bound COW plan, while the read-only block
device exposes the origin-sized latter and a complete stream confirms that
artifact length.

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

## Deliberate Non-Capabilities

This private provider slice still does not provide or select:

- physical writer fencing or fence verification; its only authority hook is
  an injected stopped-writer consumer;
- integration with the current clean checkpoint path, lifecycle facade, or
  public deployment;
- crash-prefix tail repair;
- restore or publication of a writable restore destination; or
- admission of a new writer lease or fencing epoch.

The provider and catalogue do not turn the structural request into physical
authority. A future production composition must create the stopped or fenced
authority, retain it through capture, and preserve the remaining recovery
ordering below. Cloud, filesystem-native, thin-LVM, and other snapshot adapters
also remain separate work.

## Required Future Ordering

A production crash-prefix path must preserve this safety order. A conclusively
joined complete writer boundary may satisfy the first step for a local stopped
capture; automatic stale-writer takeover requires the physical-fence branch:

1. establish and authenticate either complete writer stop or a physical fence
   for the old writer;
2. perform a fence-bound atomic capture through a concrete provider and commit
   the exact version 1 result;
3. repair the captured tail only on a separate writable generation, never on
   the committed read-only artifact; and
4. admit a writer only for that repaired generation under a strictly higher
   fencing epoch and a new lease.

No later step may compensate for skipping or weakening an earlier one. In
particular, successful structural validation, an incomplete process exit,
lease expiry, `unknown` verification, or a read-only artifact observation
cannot stand in for the required complete-stop or physical-fence authority.
