# Auth Refresh Authority Experiment

## Decision

Use a single managed-auth Codex app-server as the refresh adapter and call the
stable v2 method:

```json
{
  "method": "account/read",
  "params": {
    "refreshToken": true
  }
}
```

The authority owns the refresh token. Session workers continue to receive only
an access token through the experimental `chatgptAuthTokens` login boundary.

## Verified Call Path

The local upstream mirror and its `rust-v0.142.4` release tag contain this call
path:

```text
account/read(refreshToken=true)
→ AccountRequestProcessor::refresh_token_if_requested
→ AuthManager::refresh_token
→ request_chatgpt_token_refresh
→ persist_tokens
→ AuthManager::reload
```

Relevant upstream locations are:

- `codex-rs/app-server-protocol/src/protocol/v2/account.rs`
- `codex-rs/app-server/src/request_processors/account_processor.rs`
- `codex-rs/login/src/auth/manager.rs`
- `codex-rs/login/src/auth/storage.rs`

`account/read` is documented and is not marked experimental. It does not start
a thread, start a turn, or call the Responses API. In external auth mode the
same `refreshToken` flag is intentionally a no-op; external workers request a
replacement token only after an unauthorized backend response.

## Safe Adapter Shape

Codex file-backed auth storage writes `auth.json` in place with truncate, write,
and flush. It does not provide an atomic rename, directory sync, cross-process
lock, or generation/CAS primitive. The probe therefore does not let Codex write
the canonical authority file directly:

1. Acquire an OS advisory lock in the dedicated authority home. A small holder
   process keeps the lock while its parent pipe is open, so process death
   releases the lock automatically.
2. Read and validate canonical `auth.json` through one `O_NOFOLLOW` file
   descriptor.
3. Copy the credential into a mode `0700` staging `CODEX_HOME`.
4. Run only `initialize`, `initialized`, and
   `account/read(refreshToken=true)` against the staging home.
5. Verify account continuity, advanced `last_refresh`, a changed access token
   with at least two minutes of remaining validity, file permissions, and the
   unchanged canonical source generation.
6. Write the staged record to a synced mode `0600` temporary file, atomically
   rename it over canonical `auth.json`, and sync the parent directory when the
   platform supports it.
7. Re-read the promoted file before returning the access token to callers.

Concurrent callers inside the reference authority share one in-flight refresh
and observe the same generation. This is process-local proof only; the
production broker still needs a durable lease, fencing, encrypted canonical
storage, and database CAS.

## Live Result

The live probe passed with Codex CLI `0.142.4` and the dedicated
`.test-codex-home` login:

- two concurrent authority callers produced one refresh execution;
- the authority RPC audit contained only `initialize`, `initialized`, and
  `account/read`;
- no authority model turn was started;
- access and refresh tokens changed while the account identity remained
  continuous;
- canonical promotion used atomic rename and the resulting file remained mode
  `0600`;
- a separate `gpt-5.4` worker turn consumed the refreshed access token through
  `chatgptAuthTokens`;
- the worker never created `auth.json`.

The redacted machine-readable result is stored in
`evidence/live-auth-refresh-authority.json`.

Run the live probe only with a dedicated login that may be rotated:

```bash
CODEX_ALLOW_AUTH_MUTATION=1 npm run probe:auth-refresh:live
```

## Limitations

- A crash after the OAuth service accepts the old refresh token but before the
  staged record is promoted can still require interactive login. A changed
  staging record is preserved for manual recovery instead of automatically
  restoring the now-stale canonical token.
- `account/read` does not return the refreshed access token or a structured
  refresh error. Success must be established from the staged auth record and
  postconditions, not JSON-RPC success alone.
- The advisory filesystem lock cannot protect against an unrelated process
  that ignores it. Production correctness requires one fenced authority leader.
- `chatgptAuthTokens` remains an experimental worker API and must be covered by
  a pinned-image compatibility probe on every Codex upgrade.
