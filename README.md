# Portable Codex Runtime

Portable Codex Runtime is an experimental host runtime for moving Codex
app-server sessions between trusted machines while keeping the execution
environment, workspace, rollout state, and recovery data explicit.

The current repository focuses on compatibility probes for authentication and
interrupted-turn recovery boundaries. The planned runtime keeps refresh tokens
in a central auth authority, injects short-lived access tokens into session
workers, and treats session data snapshots separately from monotonic credential
state.

## Status

The runtime architecture is under active development. The current implementation
proves that the installed Codex app-server supports external ChatGPT access-token
injection and proves the managed refresh API choreography with an explicitly
uncontained host probe. Production managed refresh fails closed until a
per-refresh rootless containment executor is implemented. A separate loopback
probe characterizes explicit interruption, process signals, hard kills, and a
stopped-tree restore without using credentials or a real model turn.

The `chatgptAuthTokens` protocol is an experimental Codex app-server API. Pin the
Codex binary or image digest and rerun these probes before upgrading it.

## Interrupted-Turn Recovery

The recovery probe starts a real Codex app-server against a held localhost
Responses API mock. It exercises four independent scenarios:

- stable `turn/interrupt`, followed by a cold resume;
- `SIGTERM` during an active turn;
- `SIGKILL` during an active turn;
- `SIGKILL`, a stopped full-tree copy, deletion of the source tree, and restore
  at a different absolute path.

The probe verifies the explicit thread ID through both `thread/resume` and
`thread/read`. Explicit interruption persists a model-visible abort marker.
Signal and hard-kill recovery instead normalizes the stale in-progress turn to
`interrupted` without inventing that marker. The stopped-tree copy preserves
snapshot-user-accessible regular files and directories with their POSIX rwx
permission bits, plus portable UTF-8 symlink targets without following links.
Symlink permission bits are outside the modeled digest. Directory names must be
NFC-normalized. Inaccessible entries, non-ASCII cased names, case-insensitive
name collisions,
dangling relative links, relative-link
case or normalization aliases, traversal through non-directories,
resolution chains that cross protected trees, non-relocatable links, special
permission bits,
hard links (including hard-linked symlinks), sockets, FIFOs, and devices fail
closed. Ownership, ACLs,
extended attributes,
timestamps, and other unmodeled metadata are not preserved or covered by the
digest. If validation or copy fails after destination creation, the partial
destination is retained for cleanup by the trusted owner; the helper never
recursively removes a failure path that another writer could have replaced.
The copy helper requires exclusive single-writer control of its mode `0700`
owned root; concurrent mutation by another process with the same UID is not a
supported security boundary.
It is not an online, atomic, or power-loss-durable snapshot implementation.

Run the deterministic compatibility probe with the exact Codex binary from the
pinned runtime image:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery
```

If the system temporary filesystem is mounted `noexec`, set
`CODEX_RECOVERY_EXEC_ROOT` to an existing absolute directory on an executable
filesystem with a trusted owner, ancestor chain, and ACL state; the probe
creates and removes its own mode `0700` subdirectory there.
The pinned macOS/Linux runtime image must provide `/bin/ls`, which the
fail-closed ACL inspector invokes with fixed arguments.

To update the redacted evidence after an intentional runtime upgrade:

```bash
CODEX_BIN=/absolute/path/from/the-pinned-image/codex \
  npm run probe:turn-recovery -- --write-evidence
```

The command provisions no credential input and configures the model provider to
use the loopback mock. It does not impose OS-level outbound network isolation;
run it inside a network-isolated container when that stronger evidence is
required. See `docs/experiments/interrupted-turn-recovery.md` for source
evidence, exact semantics, and storage limitations.

## Managed Auth Refresh Authority

A central authority can proactively rotate its managed ChatGPT credential
without starting a model turn. The reference adapter runs stable v2
`account/read {refreshToken:true}` against an isolated staging `CODEX_HOME`,
verifies the rotated record, and atomically promotes it into the dedicated
authority home. Concurrent in-process callers share one refresh execution.

The live probe intentionally mutates the dedicated login and then performs a
separate worker turn with the refreshed access token:

```bash
chmod 700 .test-codex-home
CODEX_BIN=/absolute/path/from/the/pinned-image/codex \
  CODEX_ALLOW_AUTH_MUTATION=1 \
  CODEX_ALLOW_UNCONTAINED_AUTH_PROBE=1 npm run probe:auth-refresh:live
```

Do not point this command at the default user `~/.codex` home or the active
`$CODEX_HOME`. The probe rejects path aliases and matching directory identities
and expects `.test-codex-home` or another dedicated authority login.
Production workers must not be able to mount, rename, or write the authority
home or its parent path; only the broker owns that single-attached volume.
See `docs/experiments/auth-refresh-authority.md` for the source evidence,
failure model, and production limitations.

## External Auth Compatibility Probe

The offline probe uses synthetic JWTs, an isolated temporary `CODEX_HOME`, and a
localhost Responses API mock. It verifies that:

- `chatgptAuthTokens` is rejected without `experimentalApi` opt-in.
- The same login succeeds with `experimentalApi: true`.
- A mocked `401 Unauthorized` triggers
  `account/chatgptAuthTokens/refresh`.
- The retried request uses the replacement access token.
- External auth does not create a worker `auth.json`.

Run the full local test suite:

```bash
npm test
```

Two external-auth app-server integration tests run when `CODEX_BIN` (or
`codex` on `PATH`) is executable. The third app-server integration test is the
full interrupted-turn recovery matrix; it requires `CODEX_BIN` to be an
explicit absolute path so the probe can bind evidence to that exact binary.
Unavailable integration tests are reported as skipped on Node-only CI runners;
the remaining tests still run normally.

The reference host app-server runtime currently supports macOS and Linux process
groups. A process can escape that group by creating a new session, so this is not
production containment for credential-bearing refresh. Windows is rejected
before reading managed credentials, creating a worker home, or spawning Codex;
`ChildProcess.kill()` alone is not treated as process-tree isolation.

Run the offline protocol probe and print a JSON report:

```bash
npm run probe:external-auth
```

Set `CODEX_BIN` to test a specific Codex executable:

```bash
CODEX_BIN=/path/to/codex npm test
```

A bare executable name such as `codex` is resolved through `PATH`; relative and
empty `PATH` entries are anchored to the launcher working directory before the
app-server switches into its isolated `CODEX_HOME`. A relative value containing
a path separator, such as `./bin/codex`, is likewise resolved against the
launcher working directory.

## Live External Auth Probe

The live probe reads a dedicated managed ChatGPT login from
`.test-codex-home/auth.json`, injects only its access token into a temporary
worker, and sends one fixed, non-repository prompt to the real Codex backend.
It does not pass the refresh token to the worker or modify the source auth file.

The dedicated auth home is ignored by Git. On success, the probe writes a
redacted record to `evidence/live-external-auth.json`. The tracked evidence
contains runtime metadata and the final status only; it omits credentials,
emails, complete account/workspace identifiers, and token-derived fingerprints.

Run the live probe explicitly:

```bash
npm run probe:external-auth:live
```

Optional overrides:

```bash
CODEX_TEST_HOME=/path/to/dedicated-codex-home \
CODEX_LIVE_PROBE_MODEL=gpt-5.4 \
CODEX_LIVE_EVIDENCE=evidence/live-external-auth.json \
npm run probe:external-auth:live
```

## Repository Automation

The default branch contains `.github/workflows/codex-review-gate.yml`. Pull
requests use the `codex/review-gate` check supplied by the repository template.

The retained `scripts/setup-ci.mjs` generator can add project-specific CI and
tooling later. Inspect planned writes before enabling a module:

```bash
node scripts/setup-ci.mjs --list
node scripts/setup-ci.mjs --tool github-actions --tool markdown --dry-run
```

Its generator tests are included in the default `npm test` command.

## Project Records

- Current repository state: `docs/PROJECT_STATE.md`
- Cross-workstream backlog: `docs/PROJECT_TODO.md`
- Workstream journals: `docs/project_journal/`

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.
