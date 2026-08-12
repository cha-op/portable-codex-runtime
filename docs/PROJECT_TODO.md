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
- [done] Compose provider-backed canonical writer detach behind one
  per-operation PostgreSQL advisory guard. Invoke storage only from a definite
  typed dispatch grant, validate exact detach or force-fence proof, preserve
  finalizer acknowledgement-loss replay, and fail retained ambiguous dispatch
  state closed to `BLOCKED` without replaying the provider.
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
- [done] Implement the durable launch-attempt lifecycle around exact
  generation, lease, attachment, fencing, measured-image, process, writer, and
  supervisor bindings without invoking a launcher inside that slice.
- [done] Add the logical-writer-launcher foundation: consume the original
  one-use measured-image capability only after durable `starting`, invoke an
  external launcher once, register a provisional writer before started
  finalisation, reconcile active attempts without relaunch, and add typed
  launch-stop authority plus bounded prepared/active/current-launch discovery.
- [done] Eliminate the generation-to-launch crash gap with a version 2 restore
  request that durably binds one launch intent, one serializable transaction
  that commits the generation and reserves that exact launch attempt, and a
  launcher path that consumes only the already-reserved attempt.
- [done] Compose one same-process durable writer stop with one clean capture:
  bind the complete prepared capture tuple to the exact
  `writer-launch-stop-v1` identity, validate its committed transition before
  issuing the one-use capability, reconcile same-process pre-dispatch stop
  replay, block same-session successor launch through capture, and retire local
  writer identity only after confirmed capture success.
- [done] Compose detached restore activation and recovery: source-free verify
  only an exact committed destination, bind version 1 provider attachment
  evidence without treating pathname equality as authority, atomically install
  the canonical attachment plus prepared launch, and sweep four bounded
  generation/activation/launch/current-inventory lanes without relaunching.
- [done] Add a fleet-gated capture-bound activation request that proves
  the exact current-writer stop, clean checkpoint capture, and subsequent
  release or force-fence before a distinct target restore generation can
  activate its detached destination. Preserve existing activation v1 replay.
- [done] Persist one independent generation, activation, launch-attempt, and
  current-launch recovery cursor per configured scope, and add a bounded
  single-flight runner that durably advances each settled lane before admitting
  the next. Keep the runner unscheduled and production restore fail-closed.
- [done] Add a version 3 stopped-directory restore callback that passes the
  complete authority-issued generation binding to fresh publication or
  committed-only verification while preserving the legacy version 2 callback.
- [done] Admit capture-bound activation through the separately fleet-gated
  detach-to-generation-to-capture-to-stop predecessor chain while preserving
  the old durable topology, and let the real launcher prepare and consume an
  activation-materialized prepared launch without a second reservation or
  physical launch on replay.
- [done] Close the stop-to-capture restart gap with writer-stop request V3:
  preclaim the capture operation ID before physical stop, atomically commit the
  stop and materialize its exact prepared capture, permit fresh publication
  only after one definite cold dispatch grant, and keep starting or uncertain
  recovery committed-only. Preserve the legacy same-process capability path
  and keep fresh V3 reservation default-denied by its own fleet decision.
- [done] Add a cross-process shared/exclusive restore lifecycle guard and a
  bounded production recovery scheduler so foreground prepared launch cannot
  race recovery cancellation.
- [pending] Complete production restore adapter enablement. Phase A now
  provides the caller-persisted stable plan, invocation-time default-deny fleet
  gate, and shared-lifecycle foreground composition seam across committed
  publication, durable stop/capture, canonical detach, activation, and
  prepared launch. The production-neutral assembly foundation now constructs
  that facade, its capture-only backend, three distinct lifecycle/operation
  guard pools, authority/store pool, idle bounded no-relaunch scheduler, and a
  narrow same-launcher writer-start ingress without opening the adapter. The
  PostgreSQL durable plan registry, separately gated provisioning facet, and
  private read-only foreground resolver are now complete. The deployment
  controller now owns migration-before-serving, the initial complete recovery
  sweep, restore and image-reservation admission, scheduler shutdown, and
  admitted-call drain while leaving the four low-level pools borrowed. The
  production deployment factory now owns explicit PostgreSQL connection/
  bootstrap configuration, constructs those four private pools, performs a
  point-in-time same-primary topology check before controller startup, and
  closes every pool after controller drain. The deployment-owned image-plan
  binding now resolves an authentic plan's `imagePlanId` to exact OCI bytes,
  trusted inspection, and one opaque reservation shared by foreground
  preparation and launcher revalidation. The settlement foundation now applies
  separate result deadlines and post-deadline grace periods to image resolution
  and inspection, supplies provider contract version 2 with a fresh authentic
  abort signal plus opaque invocation identity, and routes a no-settlement
  breach only to deployment-owned fatal shutdown without claiming physical
  quiet or retry authority. Activation now also keeps claim, reconciliation,
  optional first attach, and finalization inside one coordinator-owned
  per-operation guard; retained, ambiguous, or copied caller state cannot
  reconstruct a second attach dispatch. A deployment-private physical binding
  graph now gives the three supervisor methods, nine storage-lifecycle methods,
  four publication methods, and restore-destination resolver independent
  deadlines and grace periods while preserving existing durable authority.
  Next admit the operational lease budget, run the complete assembled restart/
  ambiguous-outcome/deadline matrix, construct the final public backend, and
  only then replace the fixed fail-closed `runRestore()` stub.
- [pending] Implement an ext4 or filesystem-image physical backend, followed by
  differential compression, content-addressed storage, encryption, retention,
  periodic long-goal snapshots, and cross-host restore verification.
- [deferred] Add a read-only Git Summary for user context; it is not part of
  snapshot correctness or recovery.
