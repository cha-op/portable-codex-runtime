# Project TODO

- [done] Prove a central auth authority can refresh credentials without a
  normal model turn and preserve the successful method with redacted evidence.
- [done] Characterize interrupted and killed Codex turn recovery from the
  pinned runtime and filesystem snapshots.
- [done] Define session filesystem, manifest, lease, fencing, and pluggable
  storage contracts for rootless workers.
- [done] Implement the central auth broker so session workers receive access
  tokens without mounting shared refresh-token state.
- [pending] Implement filesystem snapshots, differential compression, retention,
  and cross-host restore verification independently of auth state.
- [deferred] Add a read-only Git Summary for user context; it is not part of
  snapshot correctness or recovery.
