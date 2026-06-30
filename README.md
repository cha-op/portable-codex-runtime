# Portable Codex Runtime

Portable Codex Runtime is an experimental host runtime for moving Codex
app-server sessions between trusted machines while keeping the execution
environment, workspace, rollout state, and recovery data explicit.

The current repository focuses on compatibility probes for the authentication
boundary. The planned runtime keeps refresh tokens in a central auth authority,
injects short-lived access tokens into session workers, and treats session data
snapshots separately from monotonic credential state.

## Status

The runtime architecture is under active development. The current implementation
proves that the installed Codex app-server supports external ChatGPT access-token
injection and keeps those credentials ephemeral inside the worker.

The `chatgptAuthTokens` protocol is an experimental Codex app-server API. Pin the
Codex binary or image digest and rerun these probes before upgrading it.

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

The two app-server integration tests run when `CODEX_BIN` (or `codex` on
`PATH`) is executable. They are reported as skipped on Node-only CI runners;
the remaining tests still run normally.

Run the offline protocol probe and print a JSON report:

```bash
npm run probe:external-auth
```

Set `CODEX_BIN` to test a specific Codex executable:

```bash
CODEX_BIN=/path/to/codex npm test
```

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
