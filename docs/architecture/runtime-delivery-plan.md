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

1. **PR #1: external-auth compatibility probe**
   - Prove the pinned app-server's external access-token injection and refresh
     callback behaviour without persisting worker `auth.json`.
2. **PR #2: auth refresh authority spike**
   - Prove a central authority can refresh ChatGPT credentials without a normal
     model turn and preserve redacted evidence.
3. **PR #3: interrupted and killed turn recovery spike**
   - Record Codex behaviour for logical interruption, process signals, hard
     kills, and stopped-tree recovery.
4. **PR #4: session filesystem and storage contracts**
   - Define the session manifest, normal-directory attachment, lease, fencing,
     backend mutation, and checkpoint descriptor contracts.
5. **PR #5: auth broker MVP**
   - Implement encrypted canonical auth state, single-flight refresh,
     generation/CAS semantics, and worker token delivery.
6. **PR #6: snapshot and restore core**
   - Implement backend-neutral stopped-writer clean checkpoint orchestration
     with exact descriptor/result validation and fail-closed uncertainty.
7. **PR #7: reusable stopped-tree primitives**
   - Extract the validated copy, digest, mount, ACL, pathname, and guarded
     cleanup layer without claiming durable publication.
8. **PR #8: durable filesystem operation journal**
   - Persist exact prepared, materialized, and committed operation records and
     predetermined results with canonical replay after restart.
9. **PR #9: stopped-directory publication layer**
   - Bind journal phases to a local filesystem storage barrier, deterministic
     private staging, atomic checkpoint-bundle or restore-tree publication,
     exact readback, and pre-commit consumer isolation.
10. **PR #10: same-process stopped-writer capability**
    - Issue and authenticate a one-use object capability bound to the exact
      writer incarnation, attachment, and fence without embedding stop
      mechanics in the storage layer.
11. **PR #11: stopped-directory backend adapter**
    - Compose the journal, publication layer, capability, and snapshot core;
      then run the complete backend conformance and failure-injection matrix.

The sequence through PR #9 is complete. Later serial pull requests begin with
the same-process stopped-writer capability and backend adapter, then own
replay-only uncertain-result reconciliation, same-image resume and rollout-tail
repair, an ext4 or filesystem-image backend, differential export and
content-addressed storage, cross-host migration, and operational hardening.

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
