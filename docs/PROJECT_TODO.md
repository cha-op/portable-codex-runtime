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
- [done] Implement the durable host-local filesystem operation journal with
  canonical phase and exact committed-result replay.
- [done] Implement local stopped-directory storage barriers, deterministic
  staging, atomic checkpoint-bundle and restore-tree publication, exact
  readback, and pre-commit consumer isolation against the journal phases.
- [done] Implement the same-process stopped-writer capability coordinator with
  one trusted stop, one object-identity capability, and one snapshot callback.
- [done] Compose the journal, publication layer, one-use stop capability, and
  durable mutation-authority/catalogue seam into the stopped-directory backend
  adapter and conformance suite.
- [done] Add authenticated durable capture-attempt provenance and source-free,
  committed-only uncertain-outcome reconciliation.
- [done] Verify same-pinned-executable resume and implement offline
  rollout-tail repair for the pinned plain-JSONL framing contract.
- [done] Add a bounded OCI/Docker runnable-image and Codex-executable
  reservation plus the PostgreSQL serializable transaction, schema, and
  real-concurrency foundation for central runtime authority.
- [done] Implement the durable operation and reservation kernel for exact
  idempotent replay, conflict exclusion, and explicit uncertain-outcome
  reconciliation.
- [done] Implement database-clock writer lease acquisition and renewal plus
  exact attachment finalization with monotonic uint64 fencing epochs.
- [done] Implement release, force-fence, `FENCING`, and `BLOCKED`
  reconciliation without treating database epoch allocation as a physical
  fence.
- [done] Implement the production clean-capture mutation authority and
  checkpoint catalogue on the existing PostgreSQL schema, including
  source-free committed reconciliation and permanent attempt claims.
- [done] Implement a bounded operational recovery enumerator and service
  loop for retained `starting` or `uncertain` checkpoint capture operations.
  Recover the exact checkpoint and mutation request only from durable
  operation state, use stable artefact-root resolver configuration, and leave
  unverifiable or guard-busy attempts durably blocked for later retry.
- [done] Add the ordered checksum-bound PostgreSQL migration chain and the
  permanent relational schema foundation for canonical restore destination
  generations.
- [done] Implement typed canonical restore destination generation claim,
  dispatch, finalisation, exact replay, and recovery authority while keeping
  production restore fail-closed.
- [pending] Implement the durable launch-attempt lifecycle around exact
  generation, lease, attachment, fencing, measured-image, process, writer, and
  supervisor bindings without invoking a launcher inside that slice.
- [pending] Compose logical launcher admission from the typed generation,
  one-use measured-image capability, durable launch attempt, external launcher,
  and exact writer registration; enable production restore only after the
  complete composition is fail-closed under ambiguous outcomes.
- [pending] Implement an ext4 or filesystem-image physical backend, followed by
  differential compression, content-addressed storage, encryption, retention,
  periodic long-goal snapshots, and cross-host restore verification.
- [deferred] Add a read-only Git Summary for user context; it is not part of
  snapshot correctness or recovery.
