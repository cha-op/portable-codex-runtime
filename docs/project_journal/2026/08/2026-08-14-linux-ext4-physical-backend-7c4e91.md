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
- Kept the supported boundary to clean stopped-writer publication and manual
  fencing while making cross-host raw-image identity verifiable after an
  operator-controlled clean detach.

## Current State

- `LinuxExt4Inspector` and `LinuxExt4ImageDriver` bind object observation and
  sparse raw-image creation, `mkfs.ext4`, loop attachment, private mount,
  `syncfs`, unmount, detach settlement, and destruction to held file
  descriptors and fixed native/helper interfaces. A child-root absence is
  conclusive only when observations before and after `lstat` bind the same
  mounted object; mount loss or replacement remains an observation failure.
- `FilesystemImageProviderState` durably records prepared and committed
  operations, storage and writer state, mount/data-root identity, and
  publication-control identity. Every mutation checks its append-only ledger
  against an external monotonic head; the PostgreSQL adapter stores that head
  in migration 8 using a dedicated serializable store and pool.
- A ledger frame is not committed merely because its bytes reached disk. Cold
  recovery first binds the externally anchored prefix, then discards at most
  one complete unanchored frame or one torn final frame; multiple or malformed
  suffixes fail closed. External callbacks must return exact native Promises
  without mutable settlement hooks.
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
  reconciliation remains a repeatable observation.
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

## Safety Boundary

- The protected properties remain separate: persistent filesystem/file-handle
  identity, content and ledger stability, and access policy. Runtime
  `device`/`inode` values only bind one held-descriptor observation window;
  benign metadata churn is not treated as object replacement.
- PostgreSQL grants the one physical mutation. The injected backend validates
  and executes that exact invocation but does not mint a grant.
- A database epoch, expired lease, process exit, inaccessible path, or
  successful first detach syscall is not by itself a physical fence or settled
  detach proof.

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
- Any broader crash, automatic-fencing, or backup/distribution capability
  requires a separately scoped authority and conformance design.

## Evidence

- All test files other than `test/app-server-auth-probe.test.mjs` passed in one
  bounded local run with `node --test --test-concurrency=4`. The excluded file's
  existing watcher-dependent case failed with host-level `EMFILE` even when run
  alone, and a minimal `fs.watch()` probe against a new empty temporary
  directory failed identically. The remaining 43 cases in that file passed.
- Focused ext4 inspector, image-driver, paths, provider-state, backend,
  PostgreSQL head-anchor, publication, Podman supervisor-state, supervisor,
  physical-binding, and logical-launcher suites passed. The only focused skip
  was the Linux-only ACL path on the Darwin development host.
- JavaScript syntax checks, native C strict syntax checks, workflow YAML parse,
  project-journal validation, and tracked/untracked whitespace checks passed.
- Real loop/mount, rootless Podman, cross-host image transfer, and PostgreSQL
  integration remain CI gates because the local host is Darwin and has no
  configured `SESSION_AUTHORITY_DATABASE_URL`.

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
