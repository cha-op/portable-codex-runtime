# Project TODO

- [done] Prove a central auth authority can refresh credentials without a
  normal model turn and preserve the successful method with redacted evidence.
- [done] Characterize interrupted and killed Codex turn recovery from the
  pinned runtime and filesystem snapshots.
- [done] Define session filesystem, manifest, lease, fencing, and pluggable
  storage contracts for rootless workers.
- [done] Implement the central auth broker so session workers receive access
  tokens without mounting shared refresh-token state.
- [done] Implement backend-neutral stopped-writer clean checkpoint and restore
  orchestration independently of auth state.
- [done] Extract reusable stopped-tree validation, copy, digest, and guarded
  cleanup primitives without claiming a durable snapshot backend.
- [pending] Implement the stopped-directory backend adapter and conformance,
  including authenticated stop evidence, an fsync barrier, atomic publication,
  destination isolation, and a durable operation journal with exact replay.
- [pending] Add replay-only uncertain-outcome reconciliation, then same-image
  resume verification and rollout-tail repair.
- [pending] Implement an ext4 or filesystem-image physical backend, followed by
  differential compression, content-addressed storage, encryption, retention,
  periodic long-goal snapshots, and cross-host restore verification.
- [deferred] Add a read-only Git Summary for user context; it is not part of
  snapshot correctness or recovery.
