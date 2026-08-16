---
id: 20260814-7c4e91
title: Linux ext4 Physical Backend
status: completed
created: 2026-08-14
updated: 2026-08-14
branch: wip/filesystem-physical-backend
pr:
supersedes: []
superseded_by:
---

# Linux ext4 Physical Backend

## Summary

- Completed production-injectable Linux physical components for the PostgreSQL
  detached-restore storage, publication, and writer-supervisor seams.
- Bound ext4 serving to a host-owned long-lived private mount namespace with
  separate `rprivate` archive and session carriers; the helper verifies but
  does not create or own that propagation boundary.
- Kept the supported boundary to clean stopped-writer publication and manual
  fencing while making cross-host raw-image identity verifiable after an
  operator-controlled clean detach.

## Current State

- `LinuxExt4Inspector` and `LinuxExt4ImageDriver` bind object observation and
  sparse raw-image creation, `mkfs.ext4`, loop attachment, mount under a
  host-owned private carrier, `syncfs`, unmount, detach settlement, and
  destruction to pinned descriptor authority and fixed native/helper
  interfaces. Clean unmount closes the mounted-root descriptor before its
  non-lazy dispatch while retaining the pinned parent/direct-child authority.
  A child-root absence is conclusive only when observations before and after
  `lstat` bind the same mounted object; mount loss or replacement remains an
  observation failure.
- `FilesystemImageProviderState` durably records prepared and committed
  operations, storage and writer state, mount/data-root identity, and
  publication-control identity. Every mutation and maintenance rotation checks
  a version 2 generation head stored by the PostgreSQL migration 8 adapter
  through a dedicated serializable store and pool. The head binds monotonic
  anchor/state revisions, generation and previous-head digest, checkpoint
  boundary/digest, and the bounded active delta-log boundary/digest.
- A delta frame is not committed merely because its bytes reached disk. Cold
  recovery first binds the externally anchored active prefix and checkpoint,
  then discards at most one complete unanchored frame or one torn final frame;
  multiple or malformed suffixes fail closed. A committed delta binds its
  prepared checksum and does not repeat the request. External callbacks must
  return exact native Promises without mutable settlement hooks.
- Provider-state rotation streams all prepared and committed exact-replay
  records, current storage state, and destroyed tombstones into a new
  checksum-framed checkpoint. It creates and syncs that checkpoint and an empty
  active log, syncs the parent directory, and only then attempts the pure-
  maintenance CAS. Rotation advances the anchor revision and generation while
  retaining the logical state revision. Default 8 MiB/8,192-frame soft
  watermarks rotate before the 64 MiB/65,535-frame hard envelope, eliminating
  manual permanent active-log exhaustion; `inspectCapacity()` exposes both
  watermarks and current usage.
- Publication compares each real pre-created control lock against a persistent
  identity authorized outside the replaceable publication image. Session-image
  identity comes from committed provider state. On the archive image, the
  mount-root control and artifact-child publication control are different
  inodes with distinct persistent tuples; producer job outputs anchor both
  independently, the mount-root tuple makes the consumer's first remount
  verification-only, and the artifact-child tuple authorizes only that exact
  publication root. Neither tuple can substitute for the other.
- `Ext4FilesystemImageBackend` supplies the raw storage lifecycle, restore-
  attachment, reconciliation, and resolver surfaces. Cold-open reconciliation
  gates serving, and unsupported raw checkpoint or automatic force-fence calls
  fail closed.
- `PodmanWriterSupervisor` supplies a version 2 rootless supervisor surface
  with a digest-pinned image, sole private session bind, held filesystem
  authority, immutable revision publication, stop/join, and exact-name stopped-only cold
  reconciliation. Its immutable local state publishes each revision through
  one no-replace data-file commit point, so a crash cannot leave a permanent
  data/marker half-commit. Only the grant-bearing stop path retires a container;
  reconciliation remains a repeatable observation. Timeout, abort, and output-
  overflow failures request `SIGKILL` but settle only after the child `close`
  barrier, so the pinned attachment descriptor remains valid through process
  termination and stdio drain.
- The privileged Ubuntu producer cleanly unmounts and detaches separate session
  and archive ext4 images before upload. A consumer on a second hosted runner
  remounts the transferred bytes under those two independently supplied archive
  identities and verifies the external provider head, committed publications,
  persistent object identity, higher-epoch reattachment, clean detach, and
  destruction.
- The native helper is installed root-owned and 0750 for a dedicated service
  group, carries only `cap_dac_override,cap_sys_admin=ep`, and rejects root,
  setuid, or any real/effective/saved UID/GID split before parsing a request.
  Producer and consumer receipts bind the same non-root numeric service UID.
- Both ext4 jobs create one long-lived private mount namespace with separate
  `rprivate` archive and session self-bind roots before dropping to that
  service user. A producer barrier gates whether both live ext4 mounts are
  visible in the child namespace and absent from the parent namespace.

## Safety Boundary

- The protected properties remain separate: persistent filesystem/file-handle
  identity, content and ledger stability, and access policy. Runtime
  `device`/`inode` values only bind one held-descriptor observation window;
  benign metadata churn is not treated as object replacement.
- The exact-head provider-state cache is a hot-path optimization, not authority.
  Stable metadata can reuse cached content only for the exact same head and
  pinned objects. Metadata change triggers content replay and revalidation; it
  is not by itself evidence of content mutation.
- PostgreSQL grants the one physical mutation. The injected backend validates
  and executes that exact invocation but does not mint a grant.
- The host owns mount-namespace lifetime and exclusive propagation authority.
  The native mount-ID/mountinfo check rejects a non-private carrier but cannot
  serialize another `CAP_SYS_ADMIN` actor in the same namespace.
- A database epoch, expired lease, process exit, inaccessible path, or
  successful first detach syscall is not by itself a physical fence or settled
  detach proof.
- A Podman command deadline starts termination; it is not a safe authority-
  release deadline. If a killed child cannot be reaped, the supervisor keeps
  the pinned attachment authority rather than returning and permitting its
  descriptor number to disappear or be reused.

## Non-Goals

- No sudden power-loss, storage-controller cache-loss, or crash-prefix
  checkpoint evidence.
- The ext4 and Podman jobs prove privilege-compatible components; they do not
  claim one same-process end-to-end PostgreSQL deployment run.
- The default Podman filesystem authority protects the call-time held object
  and access policy but does not map the provider's persistent ext4 root
  identity from an opaque proof. That trusted adapter and same-process evidence
  remain separate work.
- Terminal supervisor revisions are retained for exact replay. The current
  state component has no authority-owned bounded retention or compaction path;
  production must use monitored dedicated storage until that callback exists.
- The provider-state checkpoint is a control-plane exact-replay snapshot, not a
  physical ext4 image checkpoint, published checkpoint artifact, or content
  root. Automatic rotation bounds only the active delta log. Permanent exact
  replay makes checkpoint and aggregate persistent bytes grow with unique
  operations; this slice provides no provider-state retention floor or garbage
  collection, so the host must monitor capacity and backing storage.
- No automatic stale-writer fencing, partition revocation, or successful
  `forceFence()` path; the backend declares `fencing: "manual"`.
- No differential export/compression, content-addressed distribution,
  encryption, retention or periodic-snapshot policy, registry publisher or
  signature trust, or remote image transport.

## Next Steps

- Bind the committed ext4 attachment identity into a trusted Podman filesystem
  authority and cover both components in one non-root process.
- Add terminal supervisor-state retention or garbage collection behind an
  authority-owned post-commit/quiescence callback; never put it in the
  stopped-only reconciler.
- Define an authority-safe provider-state exact-replay retention floor, or move
  permanent operation history to a PostgreSQL-indexed representation.
- Any broader crash, automatic-fencing, or backup/distribution capability
  requires a separately scoped authority and conformance design.

## Evidence

The following completed-run evidence predates the provider-state v2 rotation
change and does not attest the current head:

- All test files other than `test/app-server-auth-probe.test.mjs` passed in one
  bounded local run with `node --test --test-concurrency=4`. The excluded file's
  existing watcher-dependent case failed with host-level `EMFILE` even when run
  alone, and a minimal `fs.watch()` probe against a new empty temporary
  directory failed identically. The remaining 43 cases in that file passed.
- Focused ext4 inspector, image-driver, paths, provider-state, backend,
  PostgreSQL head-anchor, publication, Podman supervisor-state, supervisor,
  physical-binding, and logical-launcher suites passed. The only focused skip
  was Linux-only filesystem-authority coverage on the Darwin development host.
  The default-runner regression covers delayed close after timeout, abort, and
  stdout/stderr overflow; Ubuntu additionally checks that a real `/proc` held-
  directory descriptor remains visible until the child is reaped.
- JavaScript syntax checks, native C strict syntax checks, workflow YAML parse,
  project-journal validation, and tracked/untracked whitespace checks passed.
- The privileged Ubuntu producer is the runtime gate for the live parent/child
  namespace barrier and close-before-unmount behavior; Darwin cannot execute
  either Linux mount semantic locally.
- Real loop/mount, rootless Podman, cross-host image transfer, and PostgreSQL
  integration remain CI gates because the local host is Darwin and has no
  configured `SESSION_AUTHORITY_DATABASE_URL`.
- Current-head provider-state v2 focused verification passed 298/298 tests
  across provider state, ext4 backend, PostgreSQL head-anchor and serializable-
  store integration, detached-restore runtime control, and deployment fixtures.
  The provider-state subset covers checkpoint framing and exact replay,
  automatic soft/hard-boundary rotation, rotation durability and CAS
  acknowledgement loss, cache revalidation, committed-delta request
  deduplication, and `inspectCapacity()` reporting.
- All eight changed `.mjs` files passed `node --check`; project-journal
  validation and `git diff --check` passed. A full `npm test` run reproduced
  only the pre-existing watcher-dependent `EMFILE` failure in
  `test/app-server-auth-probe.test.mjs`; that file passed 43/44 in isolation,
  and an explicit run of the other 49 test files passed.
- PR #52's first current-head GitHub Actions run passed the PostgreSQL authority
  integration but exposed two Linux-only integration defects. The ext4
  producer reached `mount-ext4` after formatting a root-owned filesystem while
  the capability-bearing helper intentionally remained the non-root service
  UID; formatting now supplies the exact real `uid:gid` through mke2fs
  `root_owner` instead of expanding helper capabilities. The rootless Podman
  job reached a 30-second command-runner failure, but that run did not expose
  the exact command or durable lifecycle phase. The exact
  `start <64-hex-container-id>` command now uses no captured output because its
  output is not authority evidence; direct-CLI close/reap remains the
  settlement barrier, and exact container inspection plus the live
  attachment-object proof remain the authoritative success evidence. This
  hardening is not claimed as the root cause of that opaque CI failure.
- After those fixes, the focused ext4 inspector/image-driver and Podman
  supervisor/state suites passed, both changed JavaScript files passed
  `node --check`, the Darwin unsupported native-helper branch passed strict
  `cc` syntax checking, and full Node test discovery again reproduced only the
  same host-level watcher `EMFILE` failure. The actual Ubuntu ext4 producer,
  dependent cross-host consumer, and rootless Podman integration are pending
  the next pushed-head CI run and are not claimed by this local evidence.
- PR #52's second GitHub Actions run passed both macOS Node matrices and the
  PostgreSQL authority integration. The ext4 helper also completed its
  FD-bound mount and native post-check, then the JavaScript driver rejected the
  resulting mountinfo because Linux preserved `/proc/self/fd/<n>` as the
  display source. The driver now treats that field as display-only and binds
  mount identity through mountinfo `major:minor`, the unique canonical loop
  receipt and backing image identity, plus the ext4 root identity. Focused
  inspector/image-driver/backend tests and strict native syntax checking pass;
  real producer and cross-host consumer evidence remains pending Ubuntu CI.
- Both Ubuntu Node matrices exposed one Linux-only hostile test whose global
  `Object.prototype` poisoning continued through unrelated state/filesystem
  work after the default ACL authority had already been exercised. The test
  now injects a `spawnSync` result whose prototype alone supplies an inherited
  `error` property while status, signal, stdout, and stderr remain own data
  properties. Reaching container creation proves that the ACL authority
  consumed only those own result fields without asking unrelated Node internals
  to operate under a globally poisoned prototype. Podman state/supervisor
  focused tests and JavaScript syntax checks pass locally.
- The second rootless Podman integration still reached a 30-second runner
  failure, but its sanitized public error and top-level Node test stack do not
  identify the command or lifecycle phase. The integration now emits only a
  fixed phase label and normalized durable-state status on failure; it does not
  print argv, container IDs, paths, requests, or command output. No behavioral
  Podman change is claimed until the next CI run supplies that bounded phase
  evidence.
- PR #52's third GitHub Actions run proved that the ext4 producer passed mount,
  canonical loop observation, writable attach, the peer-namespace isolation
  probe, payload writing, and filesystem sync. Detach then failed before any
  physical mutation because the integration helper read the attachment ID from
  `attachment.attachmentId`; the validated mutation result carries it at
  `attachment.target.attachmentId`. The helper now preserves the exact
  attachment authority tuple from that target record. The dependent cross-host
  consumer remains pending a producer-successful Ubuntu run.
- The same run narrowed the rootless Podman failure to `phase=launch` with no
  durable supervisor-state record. This excludes filesystem-authority
  acquisition, claim publication, create, start, and every later lifecycle
  phase. The 30-second duration and normalized error map the remaining external
  command timeout to the pre-claim `podman info` or image-inspection call;
  local state read/claim failures have no matching timer and the diagnostic
  state read itself succeeded. The integration now performs those two exact
  read-only calls with the same executable, environment, cwd, encoding, and
  kill signal under independent five-second bounds before constructing the
  supervisor. These probes are diagnostic-only and do not change the
  production supervisor contract or claim a root cause.
- PR #52's fourth GitHub Actions run passed both Ubuntu Node matrices, closing
  the Linux-only `spawnSync` result-prototype regression. The ext4 producer
  completed checkpoint publication and then rejected restore publication
  before journal prepare because both the checkpoint source and restore request
  used writer epoch `1`. The physical fixture now models the intended authority
  sequence explicitly: source/checkpoint epoch `1`, restore publication epoch
  `2` with its own holder and lease, and cross-host writer reattachment epoch
  `3`. The operation journal's strict newer-than-source restore check remains
  unchanged.
- The aligned Podman preflight proved that `/usr/bin/podman info --format=json`
  itself exceeded five seconds with no output under the supervisor's restricted
  environment, while the workflow shell invocation had just succeeded. Podman
  4.9.3's rootless path resolves reviewed helpers such as `newuidmap` and
  `newgidmap` through `PATH`; the supervisor now adds only the fixed
  `/usr/bin:/bin` value to the normalized child environment. It still rejects
  caller-controlled `PATH` and does not inherit the ambient process
  environment. The next Ubuntu run remains the authority for this diagnosis.

- `docs/architecture/linux-ext4-physical-backend.md`
- `src/linux-ext4-inspector.mjs`
- `src/linux-ext4-image-driver.mjs`
- `src/filesystem-image-provider-state.mjs`
- `src/postgres-filesystem-image-provider-head-anchor.mjs`
- `src/ext4-filesystem-image-backend.mjs`
- `src/podman-writer-supervisor.mjs`
- `native/linux-ext4-inspector.c`
- `migrations/authority/008-filesystem-image-provider-heads.sql`
- `integration/linux-ext4-physical-backend.mjs`
- `integration/podman-writer-supervisor.mjs`
- `.github/workflows/test.yml`
