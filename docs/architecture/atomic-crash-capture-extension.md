# Atomic Crash-Capture Extension

## Status and Scope

This document defines the dormant, provider-neutral version 1 extension for
capturing one `crash-prefix` checkpoint at an atomic storage boundary. The
extension gives a future authority a closed request, result, and committed
verification vocabulary. It does not make any currently assembled runtime or
public deployment capable of taking that checkpoint.

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
object, reads its version, backend ID, capability, and both extension methods
as data properties, and closes over those method values in a branded frozen
facade. Reapplying `createAtomicCrashCaptureBackendFacade()` to that facade in
the same process returns the same object and does not look the methods up
again.

`prepareAtomicCrashCapture()` separately binds one normalized request to one
frozen, object-identity token. `capturePreparedAtomicCrashCheckpoint()`
consumes that token synchronously before invoking the provider, so provider
rejection or acknowledgement loss cannot reopen it. A second dispatch through
the same token is rejected.

These rules prevent a mutable backend object, prototype replacement, accessor,
or method swap from changing the implementation held by one facade, and bind
one token to one dispatch attempt. They do not survive a process restart and
create no provider catalogue entry. The process-local brands are not a global
registration of the underlying implementation, and independently creating a
new facade or token is not a durable deduplication mechanism. A future
composition must reconstruct its authority from durable provider records, not
from either process-local brand.

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

This extension does not provide or select:

- an LVM, device-mapper, filesystem, cloud, or other atomic snapshot adapter;
- provider operation journaling or a committed-result catalogue;
- physical writer fencing or fence verification;
- integration with the current clean checkpoint path, lifecycle facade, or
  public deployment;
- crash-prefix tail repair;
- restore or publication of a writable restore destination; or
- admission of a new writer lease or fencing epoch.

The structural validators and same-process facade therefore cannot certify
physical authority. That certification remains the responsibility of a future
composition together with a concrete provider implementation and its durable
catalogue.

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
