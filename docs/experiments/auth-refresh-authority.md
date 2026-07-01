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

1. Open a regular single-link lock file with `O_NOFOLLOW`, pass that exact file
   descriptor to an OS advisory lock holder, and keep the holder alive through
   its parent pipe. Linux opens `/proc/self/fd/3` in `flock` command mode so the
   bare descriptor is not misparsed as a path. Linux also uses `--no-fork` and
   a dedicated conflict exit code; both platforms isolate the holder in a
   process group that is synchronously terminated on timeout or release.
   Process death releases the lock automatically, while inode checks reject
   lock-path replacement.
2. Read and validate canonical `auth.json` through one `O_NOFOLLOW` file
   descriptor.
3. Hold an open authority-directory guard for the transaction and revalidate
   its device/inode identity before every critical pathname operation. Refuse
   refresh when an earlier promotion candidate or staging attempt requires
   manual recovery.
4. Copy the credential into a mode `0700` staging `CODEX_HOME`, fsync both
   files and the staging directory chain, and fail before OAuth rotation if
   those recovery markers cannot be made durable.
5. Recheck the authority lock immediately before the RPC and kill the isolated
   app-server process group if the independent holder exits. Because a remote
   OAuth rotation may already have committed, any loss after refresh starts is
   non-retryable: retain the durable staging attempt as a recovery sentinel so
   the next authority run cannot reuse the old refresh token. Run only `initialize`,
   `initialized`, and
   `account/read(refreshToken=true)` against the staging home.
6. Verify workspace and user continuity, advanced `last_refresh`, a changed
   access token with at least two minutes of remaining validity, file
   permissions, and the unchanged canonical source generation.
7. Write the staged record to a synced mode `0600` temporary file, ask the
   actual lock-holder process to atomically rename it over canonical
   `auth.json`, and sync the held authority-directory descriptor when the
   platform supports it. A lost holder acknowledgement first terminates and
   waits for the holder executor, then fails closed as a non-retryable uncertain
   commit and preserves every recovery copy that still exists.
8. Re-read the promoted file while the lock and authority-directory guard are
   still valid. Return only the exact canonical bytes, never a staged fallback.

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
- access and refresh tokens changed while workspace and user identities
  remained continuous;
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

- A crash or lock loss after the OAuth service accepts the old refresh token but
  before the staged record is promoted can still require interactive login.
  The staging attempt is preserved even when its local token bytes did not
  change, because the remote refresh outcome is unknowable after forced local
  termination. Operators must inspect the reported recovery path and
  deliberately promote, reauthenticate, or securely remove old attempts;
  automatic age-based reaping could delete the only valid rotated token or
  permit reuse of an already-consumed token.
- A crash can also leave `.auth.json.next-*` after that file is synced but
  before rename. The next authority run reports all matching promotion and
  staging candidates as `recovery_required` and does not consume the canonical
  refresh token again. It never guesses which candidate to promote or delete.
- `account/read` does not return the refreshed access token or a structured
  refresh error. Success must be established from the staged auth record and
  postconditions, not JSON-RPC success alone.
- The advisory filesystem lock cannot protect against an unrelated process
  that ignores it. Production correctness requires one fenced authority leader.
- Node.js does not expose `openat`/`renameat`; directory-FD identity guards close
  deterministic replacement attacks but cannot eliminate the final pathname
  TOCTOU window. The authority volume must therefore be single-attached, its
  parent chain must not be writable or renameable by workers, and only the
  broker may mount the canonical credential directory.
- The reference lock backend requires `/usr/bin/lockf` on macOS and `flock`
  from util-linux plus procfs on Linux. Fixed runtime images must include and
  test the matching backend. The repository test workflow exercises both
  `macos-latest` and `ubuntu-latest`.
- Directory-sync, cleanup, or lock-release errors after a verified canonical
  promotion are returned as committed warnings, preventing callers from
  rotating a second token in response to an already committed refresh. A
  canonical reread or exact-byte verification failure is non-retryable and
  preserves staging for operator inspection instead of returning a staged
  access token as though promotion had succeeded.
- Refresh results contain secret-bearing properties for the broker adapter.
  Those properties are non-enumerable to reduce accidental JSON/log exposure,
  but callers must still avoid logging or spreading the result object.
- `chatgptAuthTokens` remains an experimental worker API and must be covered by
  a pinned-image compatibility probe on every Codex upgrade.
