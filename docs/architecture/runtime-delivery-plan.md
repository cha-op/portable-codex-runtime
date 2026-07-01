# Runtime Delivery Plan

## Objective

Build a portable Codex app-server runtime whose session filesystem can move
between trusted hosts while authentication remains a separate monotonic control
plane.

## Delivery Invariants

- Deliver one pull request at a time.
- After each squash merge, update local `master` and create the next branch from
  that new commit.
- Run the complete repository test suite for every pull request.
- Require GitHub Codex review, an independent Codex PR review, and an offline
  frozen-diff review before merge.
- Resolve every pull-request conversation before merge.
- Keep credentials, untracked private files, and unrelated repositories outside
  review and evidence artifacts.
- Serialize live tests that use `.test-codex-home`; copied refresh-token state is
  not an independent test credential.

## Pull Request Sequence

1. **Auth refresh authority spike**
   - Determine whether a central authority can refresh ChatGPT credentials
     without a normal model turn.
   - Preserve a reproducible successful implementation and redacted evidence.
   - Verify serialized refresh, generation continuity, worker token injection,
     and explicit `reauth_required` failure behavior.
2. **Interrupted and killed turn recovery spike**
   - Record Codex behavior for logical interruption, `SIGTERM`, `SIGKILL`, and
     recovery from a filesystem snapshot.
   - Treat the pinned Codex image as the owner of rollout recovery semantics.
3. **Session filesystem and storage contracts**
   - Present a normal directory to the rootless worker.
   - Define the session manifest, storage backend, lease, and fencing contracts
     without requiring the worker to attach or mount a raw block device.
4. **Auth broker MVP**
   - Implement encrypted canonical auth state, single-flight refresh,
     generation/CAS semantics, worker token delivery, and reauthentication.
5. **Snapshot and restore core**
   - Implement quiescing, filesystem snapshots, manifests, restore, and
     same-image thread resume verification.
6. **Differential export and content-addressed storage**
   - Implement chunking, compression, integrity checks, encryption adapters,
     atomic manifest publication, and restore verification.
7. **Cross-host migration end-to-end verification**
   - Exercise graceful migration and disaster recovery with explicit thread IDs
     and fencing epochs.
8. **Fault injection and operational hardening**
   - Cover stale hosts, interrupted uploads, corrupted chunks, retention, and
     recovery runbooks.

Later pull requests may be split further when an experiment reveals a narrower
stable boundary. They must not be combined in a way that hides an experimental
result inside production implementation.

## Parallel Work Within One Pull Request

The repository keeps pull requests serial, but one pull request can use parallel
read-only or isolated tasks for:

- upstream Codex source analysis;
- test-matrix and failure-mode design;
- security, evidence-redaction, and compatibility review.

The integrating agent owns shared files, live-auth scheduling, final code,
delivery gates, and merge orchestration.

## Deferred Work

- **Git Summary** is intentionally deferred. It may later report read-only user
  context such as branch, commit, cleanliness, and insertion/deletion counts,
  but it is not part of snapshot correctness or recovery.
