---
id: 20260829-a93f61
title: Private Complete-Stop Atomic Crash Capture
status: completed
created: 2026-08-29
updated: 2026-08-29
branch: wip/atomic-crash-capture-composition
pr:
supersedes: []
superseded_by:
---

# Private Complete-Stop Atomic Crash Capture

## Summary

- Add a separately branded module-private PostgreSQL launcher facet for one
  exact version 1 atomic crash-capture request without changing the existing
  clean launcher facade or clean stop-operation identity.
- Assemble that facet privately with the classic LVM provider so the opaque
  stopped-writer capability remains within the private assembler boundary and
  is consumed only while one freshly claimed snapshot runs.
- Permit acknowledgement recovery only through source-free committed
  verification, and retire the local stopped-writer blocker only after the
  exact committed result is known.

## Protected Authority

- The durable stop operation is domain-separated from clean capture and binds
  the launch attempt plus the complete atomic request, including capture,
  checkpoint, mutation, attachment, storage, lease, holder, and fencing
  identities.
- The launcher retains the opaque capability together with the registered
  attachment, canonical lease, process and writer incarnations, and stop
  operation. The composition and provider never receive the writer handle or
  clean stop resolution.
- The launcher exports neither the raw facet nor an accessor to complete,
  authority-consume, or retirement methods. The ordinary factory still
  returns only the nine-method launcher facade. A separate launcher-owner
  factory additionally issues one branded, zero-key
  `atomicCrashCaptureAssembler`; only that capability may ask the safe public
  composition factory to bind its chosen backend, catalogue, and driver to the
  matching private facet. Ordinary facade holders and structural clones cannot
  treat caller-built collaborators or results as committed proof.
- A fresh provider claim consumes the capability exactly once around the LVM
  driver callback. A committed catalogue replay consumes no authority and
  instead revokes the unused capability before retirement.

## Uncertainty Boundary

- Preparation captures the exact provider and request before stop authority is
  presented.
- Provider acknowledgement loss may trigger one immediate source-free read.
  `committed` admits exact retirement; `unknown` or failed verification retains
  the blocker and never reopens stop, authority consumption, or snapshot
  dispatch.
- Later reconciliation is same-process and request-exact. It performs only
  committed verification and cannot reconstruct an opaque capability after
  restart.

## Safety Boundary

- The composition accepts only a conclusively joined complete stop for the
  current local writer. It adds no physical stale-writer fence or takeover.
- The clean checkpoint path, lifecycle facade, assembled ext4 backend, and
  public deployment remain unchanged. Public capability discovery continues to
  report `atomicPointInTimeCheckpoint: false` with manual fencing.
- Tail repair, detached writable restore, higher-epoch writer admission,
  publication, retention, export, distribution, compression, encryption, and
  host/controller/drive cache-loss evidence remain separate work.

## Validation

- `node --check` passed for the launcher, private composition, and focused
  launcher test module.
- All 173 launcher tests passed, including the real launcher/coordinator/LVM
  composition fixture. The related crash core, LVM provider, storage-contract,
  and stopped-writer coordinator suites passed all 264 tests.
- The default catalogue/LVM integration invocation passed its catalogue path
  and skipped the privileged LVM case on macOS as expected. Real PostgreSQL and
  privileged LVM execution remain required Linux CI evidence.
- The unfiltered repository suite had one failure: the unchanged live-auth
  watcher exhausted the host file-descriptor limit with `EMFILE`. The complete
  suite then exited zero with only that exact test name skipped.
- Project-journal validation, syntax checks, and `git diff --check` passed.
- The sole fresh-context local review first identified the formerly exported
  raw retirement surface. Its same-lane rerun then found that accepting the
  ordinary facade plus caller-selected collaborators still promoted those
  collaborators to retirement authority. The fixes make the facet
  module-private and require a separately branded launcher-owner assembler
  capability, with export-shape, forgery, clone, proxy, foreign-launcher, and
  cross-composition concurrency regression coverage.

## Evidence

- Launcher facet, owner capability, and safe factory:
  `src/postgres-logical-writer-launcher.mjs`
- Public composition entrypoint:
  `src/postgres-atomic-crash-capture-composition.mjs`
- Private assembler: `src/postgres-atomic-crash-capture-composition-internal.mjs`
- Provider: `src/lvm-atomic-crash-capture-provider.mjs`
- Architecture: `docs/architecture/atomic-crash-capture-extension.md`
