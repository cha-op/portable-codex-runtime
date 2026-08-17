---
id: 20260814-7c4e91
title: Linux ext4 Physical Backend
status: completed
created: 2026-08-14
updated: 2026-08-17
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
  reconciliation remains a repeatable observation. The default authority pins
  the parent directories and mirrors them through a temporary holder in
  Podman's rootless namespace; configured and running inspections bind that
  exact procfd to stable container ID/PID evidence. New create receipts admit
  only complete 64-hex container identities. Ordinary failures observed before
  direct-child exit terminate the isolated process group; every failure settles
  only after direct close plus a kernel `ESRCH` group-absence proof and never
  signals the frozen PGID after exit. A dispatched exact start advances only
  on zero; every post-spawn non-success stays pending so an escaped conmon/crun
  cannot outlive the holder authority. Holder shutdown independently requires
  its wrapper close and group absence before parent descriptors are released.
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
  release deadline. If an ordinary killed command group cannot be proved
  quiescent, the supervisor keeps the pinned attachment authority. The exact
  start mutation has no post-dispatch caller timeout: even a naturally nonzero
  CLI can leave a separately grouped conmon behind, so non-success remains
  pending until an external process/cgroup fence exists. This protects object
  identity and FD lifetime, not directory content or ordinary metadata churn.

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
- PR #52's fifth GitHub Actions run passed all four Node matrices and the
  PostgreSQL authority integration. The ext4 producer completed writable
  attachment, checkpoint and higher-epoch restore publication, clean unmount,
  and mount-absence verification before `detach-loop-settle` timed out. The
  helper had incorrectly required `/run/udev/data` to contain a post-detach
  `diskseq`: systemd's `Q:` record is a current tag, while Linux 6.8 emits the
  loop change event before incrementing `diskseq`, so that asynchronous cache
  has no promised matching record. Settlement now relies on the exact loop
  `rdev`, `LOOP_GET_STATUS64 == ENXIO`, a strictly newer `diskseq`, absent
  sysfs backing state, the driver's exact-image zero-mapping scan, and retained
  object/access-policy revalidation. The next privileged producer and
  dependent consumer runs remain the runtime authority for this change.
- The same run disproved fixed `PATH` as a sufficient explanation for the
  Podman timeout: the temporary five-second restricted-environment preflight
  still timed out before the real supervisor ran. The fatal probes have been
  removed. One non-blocking, output-redacted 2-by-2 diagnostic now compares
  shell versus Node `execFile` and inherited versus restricted fixed-`PATH`
  environments after the image build, all from `/` under eight-second and
  one-MiB output bounds. It records only a fixed label, elapsed time,
  exit/signal, and byte counts, then always continues to the real supervisor's
  existing 30-second production path. No additional ambient environment value
  is admitted to production without that evidence.
- PR #52's sixth GitHub Actions run passed the real ext4 producer and dependent
  cross-host consumer, proving clean loop detach without the asynchronous udev
  database conjunct. All four bounded Podman diagnostics—shell and Node,
  inherited and restricted environments—timed out in `podman info` with no
  stdout, while the same job immediately removed the built image in about
  46 milliseconds. That evidence rejects a global Podman or storage lock and
  isolates the failure to the broad `info` inventory path, without claiming a
  particular internal helper or lock.
- The temporary diagnostics are removed. The supervisor now requires equal
  non-root real/effective user IDs, prefixes every lifecycle command with
  `--remote=false`, and proves the local rootless ABI through the exact bounded
  `unshare /usr/bin/true` command before publishing a new claim. Podman 4.9.3
  rejects `unshare` for its remote client and rootful engine, so command success
  directly protects the local-rootless property without parsing the unrelated
  host/store inventory returned by `info`. The workflow build, inspect, and
  cleanup calls use the same explicit local mode. The next Ubuntu run remains
  the runtime authority for this replacement.
- The next bounded diagnostic proved the remaining launch failure occurred in
  `podman create` after the durable `preparing` claim and before any container
  existed. Podman 4.9.3 dereferences bind sources with `statfs` after entering
  its rootless user namespace, so it cannot use a procfd owned by the parent
  Node process in the ancestor namespace. The built-in authority now starts a
  lifecycle-scoped FD holder through the same local `podman unshare` execution
  domain. Paths cross one bounded private-stdin frame rather than argv; the
  helper's bounded PID/FD/`dev`/`ino` receipt and heartbeats are independently
  checked from the parent before create and start.
- Create is followed by a non-running inspection whose external `created`
  status is Podman 4.9.3's encoding of its internal Configured state and that
  requires the exact holder procfd as the sole read-write `rprivate` session
  bind. Image `Config.User` must be a canonical non-root numeric `uid:gid` and
  drives the exact `keep-id:uid=...,gid=...` mapping. Stable exact-container
  ID/PID inspections bracket the live session-object and ACL proof before the
  helper is reaped. The Linux integration additionally checks that the writer's
  `0600` marker is owned by the current service UID/GID. Local focused tests
  cover malformed and mismatched receipts, timeout cleanup, benign child churn,
  replacement and policy changes, holder death, and configured-source drift;
  the actual rootless namespace path remains gated by the next Ubuntu run.
- A final process-lifetime audit found that direct-child `close` was not enough
  on command failure: a child that closed stdio could remain alive and use the
  holder source after the parent released its directory FDs. Ordinary Podman
  CLIs now run in isolated process groups; abort, timeout, output overflow, and
  other failures observed before exit kill the group. A natural nonzero result
  observed after exit only waits. All failures require direct close plus a
  proved kernel `ESRCH` group-absence result before settlement, and no path
  signals a frozen numeric PGID after exit. A partial `/proc` view
  or visible-zombie subset is not accepted because it could hide a live member.
  Exact full-ID `start` is not force-cancelled after dispatch because Podman
  4.9.3 launches conmon in a separate process group. A zero exit resumes exact
  container and live-bind
  proofs; every post-spawn error, signal, or nonzero exit stays pending because
  Podman's internal runtime-create timeout can fire while conmon/crun still
  resolves the source and this supervisor has no authenticated conmon fence.
  Tests use a
  closed-stdio descendant to distinguish group quiescence from stdio drain,
  hold an aborted start past its command deadline until a test-owned zero exit,
  and require a nonzero dispatched start to retain authority indefinitely. The
  resulting unbounded wait is an explicit availability tradeoff, not a content-
  stability or metadata-change detector.
- The same final audit applied the group fence to the namespace holder itself.
  Direct wrapper `close` no longer authorizes parent-FD release while a same-
  group helper may survive with closed stdio. Forced cleanup signals the group
  only before the wrapper's `exit` event proves its process identity has ended;
  after exit it waits through direct close for kernel `ESRCH` without
  signalling a potentially reused PGID. A wrapper-plus-helper fixture must
  prove group absence before authority close.
- Head `100d580` passed the PostgreSQL integration and both real ext4 producer
  and cross-host consumer jobs in Test run `31981977882`. Its two Ubuntu Node
  jobs exposed a Linux-only fixture race: the fake holder could publish its
  receipt and fail before Node observed `exit`, so forced cleanup legitimately
  killed the descendant before the fixture's leader marker appeared. The
  fixture now consumes the first heartbeat before requesting leader exit,
  labels that marker only as an exit request, and registers its marker-based
  bounded cleanup before the fixture-root cleanup. It never signals an
  unbound numeric PID.
- The same run's Podman lifecycle stayed pending from `00:25:49Z` until the
  already-failed run was cancelled at `00:35:01Z`, without reaching the
  integration catch/finally diagnostics. This matches the explicit exact-
  `start` fail-stop boundary above. The conformance integration now has a
  lifecycle-wide 45-second watchdog that emits only fixed durable/Podman state,
  running, and PID categories, followed by a 60-second hard exit. Its workflow
  step has a two-minute outer bound and its always-run image cleanup has a
  separate one-minute/30-second command bound. The watchdog does not inject an
  `AbortSignal` into the production `start` or settle that promise through its
  normal cleanup path; it terminates only the ephemeral conformance process.
  That exit closes the holder control pipe and may release the synthetic test
  authority, so GitHub's disposable runner teardown is the outer diagnostic
  fence, not evidence of a production recovery path.
- On the final local bytes, supervisor/state focused tests passed 59/66 with
  seven Linux-only skips. The supervisor/state, logical-launcher, and physical-
  binding set passed 226/233 with the same seven skips. Full Node discovery with
  only the independently reproduced `chatgptAuthTokens` watcher-`EMFILE` case
  skipped passed 2,912/2,922 with ten skips and no failures. The supervisor,
  holder helper, integration, and test files passed `node --check`; the bundled
  project-journal validator and tracked/untracked whitespace checks passed.
  Darwin did not execute the new Linux procfd/process-group tests or the real
  rootless Podman holder path; the next Ubuntu job remains their runtime gate
  and will classify any retained start fail-stop before its hard bound.
- Head `0be49ca` passed every Node matrix, the PostgreSQL authority
  integration, the real ext4 producer, and the dependent cross-host consumer
  in Test run `31983148280`. The Linux holder/process-group fixtures therefore
  passed on both supported Node versions. The sole failure was the bounded
  Podman watchdog: durable state and exact inspection were both `created`,
  with `Running == false` and `Pid == 0`. This proves create, the durable
  `preparing -> created` transition, the exact configured-source inspection,
  and the pre-start holder revalidation completed before the exact full-ID
  `start` dispatch remained unsettled. The snapshot is compatible with either
  a still-running Podman CLI or its non-success path entering the deliberate
  fail-stop wait; it does not yet distinguish a procfd, user-namespace,
  conmon, or OCI runtime failure. The next diagnostic classifies Podman's
  persisted `State.Error`, `ExitCode`, and `ConmonPid` into fixed labels only;
  it never publishes the raw error, container ID, source path, procfd, or stderr and
  does not weaken the production authority-release boundary.
- Head `770b92d` again passed every job except the bounded rootless Podman
  integration in Test run `31983924476`. Its exact diagnostic was durable and
  inspected `created`, non-running with zero PID, plus a non-empty
  OCI-context `State.Error`, zero exit-code field, and absent `ConmonPid`.
  Podman 4.9.3 persists an internal `Container.Start` error in that field
  before releasing the container lock. This proves the internal start path
  recorded a failure while the supervisor remained pending; it does not alone
  prove that the direct CLI had closed or that the runner had entered its
  post-close fail-stop branch. The coarse `oci-other` label does not identify
  the failed protected operation. The next bounded pass splits the same
  in-memory error into fixed
  runtime, operation, and errno allowlists covering procfd, mount propagation,
  user-namespace mappings, security, cgroup, network, process, and rootfs
  paths. It also reports only whether Podman recorded a non-empty OCI config
  path and fixed labels for the optional `OCIRuntime` inspect field and cgroup
  manager. An absent runtime field can still mean Podman selected its default.
  Presence proves only that `saveSpec` recorded the path; it does not validate
  the file or prove that conmon started. Absence narrows the error to an
  earlier prepare/spec stage. Raw errors, IDs, and paths remain private, and
  the production launch behavior is unchanged.
- Head `799b776` again passed both Node versions on macOS and Linux, the
  PostgreSQL integration, the physical ext4 producer, and the cross-host
  consumer in Test run `31984917265`. The bounded Podman result was still
  durable and inspected `created`, non-running with zero PID, but refined the
  error to runtime `conmon`, unknown operation and errno, present OCI config,
  configured runtime `crun`, cgroup manager `cgroupfs`, zero exit-code field,
  and absent `ConmonPid`. This tuple most closely matches Podman 4.9.3's
  `conmon failed: %w` wait wrapper, but does not yet distinguish `cmd.Start`
  failure, initial conmon exit, or signal, and still cannot authorize release
  of the held filesystem authority.
- The next diagnostic therefore adds a fixed `conmonOutcome` before consulting
  any log. Only an exact conmon wait-exit outcome consumes the CI system
  journal. The integration records a private launch boundary, then reads at
  most 64 KiB for five seconds from the current boot, error priority, exact
  conmon syslog identifier, current host UID, and syslog transport. A `_COMM`
  value is checked when journald captured one but is not required for a
  short-lived fatal process. In memory the diagnostic further
  requires the exact first 20 characters of the uniquely inspected full
  container ID and emits only fixed `conmonStage` and `conmonErrno` labels.
  The cursor, ID, numeric exit status, raw message, path, PID, stdout, and
  stderr are never serialized. The CI-only read uses passwordless `sudo` on
  GitHub's disposable runner and does not change the production command or
  recovery contract. Conmon failures before `set_conmon_logs`, missing or
  delayed journal records, permissions, overflow, and malformed JSON remain
  `absent` or `unreadable`; those labels do not prove that no fatal occurred.
  The diagnostic backstop is now 65 seconds inside the existing two-minute
  workflow bound.

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
