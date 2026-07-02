# Auth Broker MVP

## Scope

The Auth Broker MVP separates monotonic authentication authority from every
portable session volume. It implements two library boundaries:

1. an encrypted, generation-fenced canonical state store; and
2. a broker that performs refresh single-flight, publishes only committed
   generations, and produces exact Codex external-token protocol payloads.

The MVP is intentionally a single trusted broker process supervised as a
singleton. It does not implement distributed leader election, multi-host
database transactions, a KMS, or the production rootless executor that runs
the credential-bearing Codex refresh adapter.

## Upstream Codex Basis

The design remains pinned to Codex source commit
`db887d03e1f907467e33271572dffb73bceecd6b` and the measured
`codex-cli 0.142.4` runtime.

- Managed refresh uses stable v2
  `account/read { "refreshToken": true }` and does not start a thread, model
  turn, or Responses API request.
- Worker login uses experimental
  `account/login/start { "type": "chatgptAuthTokens", ... }`.
- After a worker receives `401 Unauthorized`, app-server sends
  `account/chatgptAuthTokens/refresh` with `reason: "unauthorized"` and the
  previous account ID. Its response contains only `accessToken`,
  `chatgptAccountId`, and `chatgptPlanType`.

The experimental worker boundary remains compatibility-tested and pinned to
the runtime image. It is not a stable replacement for the broker's own
generation and identity checks.

## Encrypted Canonical Store

`EncryptedAuthStateStore` stores one machine-owned canonical envelope under a
private, broker-owned authority directory. The key provider is injected by the
trusted supervisor and returns 256-bit secret `KeyObject` instances; keys are
never loaded from the state directory, worker environment, or portable
session filesystem.

The public envelope contains only non-secret routing and cryptographic data:

```json
{
  "formatVersion": 1,
  "algorithm": "aes-256-gcm",
  "authorityId": "authority-001",
  "keyId": "auth-key-2026-07",
  "nonce": "<base64url>",
  "ciphertext": "<base64url>",
  "tag": "<base64url>"
}
```

AES-GCM additional authenticated data binds the format, algorithm, authority,
and key ID. The encrypted record binds a canonical uint64 generation, the
base generation, a commit ID, operation type, and an opaque exact UTF-8 broker
payload. The store exposes that payload only as a non-enumerable property.

Each compare-and-swap:

1. validates the private authority directory, its trusted ancestor chain, and
   extended-ACL policy;
2. holds and revalidates a directory identity guard and advisory transaction
   lock;
3. reads and authenticates the current canonical generation;
4. rejects any final newline-terminated encrypted envelope above the canonical
   read limit before creating a temporary file;
5. writes a same-directory mode `0600` candidate with a fresh 96-bit nonce;
6. syncs the candidate, performs the lock-held rename, syncs the held
   directory, and re-reads the committed record; and
7. returns only after generation, commit ID, payload, and key ID match.

Directory and ancestor ACL results are cached only inside one transaction.
The authority directory's device and inode are pinned from a constructor-time
`O_NOFOLLOW` directory descriptor and shared by same-process stores with the
same coordination key. Every transaction requires both that identity and the
constructor-time canonical path; replacement, remount, or runtime symlink
drift fails closed before lock acquisition.
Authority-directory evidence is reused only while its directory type, device,
inode, owner, group, mode, and nanosecond change time remain identical.
Ancestor evidence uses the same identity and permission fields but not change
time, because unrelated sibling activity legitimately changes a shared
ancestor's timestamp. Ancestor evidence is instead invalidated and rechecked
after lock acquisition, before candidate creation, immediately before rename,
and immediately after rename. The cache is never shared across transactions;
held-directory identity, pathname identity, owner, mode, and ancestor checks
still run at every fencing point.

Process-local coordination keys encode the constructor-time canonical realpath
and authority ID as an unambiguous JSON tuple. Filesystem aliases collapse to
one queue, while delimiter-bearing paths or authority IDs cannot merge
unrelated refresh or storage queues.

An exact retry of the same commit ID, base generation, operation, and payload
is a replay. Any changed content or precondition is a conflict. Orphan
promotion candidates block both reads and writes for operator recovery; they
are never promoted or deleted by age. Once rename may have happened, every
unproven failure is non-retryable `commit_outcome_uncertain`.
Public reads and exact CAS or rotation replays sync the held directory again
before returning. Visibility of a renamed canonical file therefore never
substitutes for directory-sync proof, including after a fresh broker starts.

Key rotation is itself a generation-incrementing CAS. The old key must remain
available until the new envelope has passed directory sync and canonical
readback verification.

## Broker State and Generation

The encrypted broker payload has one of three public states. Refresh uses a
durable `recovery-required(refresh_in_progress)` reservation before any remote
call:

```text
ready(N)
  └── reserve ──> recovery-required(refresh_in_progress, N+1)
                    ├── validated candidate ──> ready(N+2)
                    ├── permanent account loss ──> reauth-required(N+2)
                    └── uncertain/invalid result ──> recovery-required(N+2)
```

A `ready` payload contains the exact Codex auth JSON plus its structurally
cross-checked access token, account ID, user ID, plan type, and canonical
expiration timestamp.
The broker decodes both JWT payloads and requires `auth_mode: "chatgpt"`, access
token `exp`, account, user, and plan claims to match those top-level fields.
Every JWT segment must use canonical unpadded base64url, and payload decoding
rejects invalid UTF-8 instead of normalising it.
This is structural continuity validation, not JWT signature verification; the
trusted OAuth adapter remains responsible for obtaining tokens from the pinned
Codex/provider path.
Blocked payloads contain an allowlisted reason plus the refresh reservation's
unique non-secret owner ID and SHA-256 digests of the source account, user,
access-token, and refresh-token identities. The ID prevents ABA ownership
confusion; the access digest lets a late caller distinguish a changed access
credential from a trusted pre-dispatch restore, the account and user digests
preserve identity continuity, and the token digests fence explicit recovery
against republishing either source token. None can be used as a credential. The
reservation therefore
removes the old auth JSON from canonical state before OAuth can consume its
refresh token. A crash or
unreconciled storage failure leaves a non-reusable durable block instead of an
old credential. The durable store generation is the only published generation;
the spike's process-local counter
is not reused.

`AuthBroker` provides:

- credential installation or explicit reauthentication through CAS;
- fenced crash recovery for an exact refresh reservation;
- non-secret status snapshots;
- TTL-aware token grants;
- generation-aware refresh grants for worker `401` callbacks;
- exact `chatgptAuthTokens` login and refresh response objects; and
- fixed, secret-free public error metadata.

Access tokens and account IDs on grants are non-enumerable to reduce accidental
JSON or spread exposure. Grants never contain raw auth JSON, refresh tokens, ID
tokens, or user identity metadata; only the reservation owner retains the
pre-reservation credential in memory for the adapter call. This is still only
defense in depth: worker protocol payloads must contain the access token and
must travel over a restricted local channel or an equivalent authenticated
transport.

## Refresh Transaction

All broker objects targeting the same store coordination key and compatible
fixed authority configuration share one process-local in-flight refresh.
Concurrent objects with different fixed safety TTLs or adapter identities fail
closed instead of sharing or starting a second refresh. Object adapters are
identified by both their owner object and captured unbound refresh method, so
replacing that method cannot merge incompatible brokers:

The store reference, coordination key, refresh adapter identity, TTL floor,
clock, and opaque-ID source are captured in private fields at construction.
Later public-property shadowing cannot change that authority configuration.
Grant option envelopes are validated as plain own-data objects before any
property or store access, so malformed accessors and proxies stay inside the
broker's fixed `invalid_request` error boundary.

1. Read and decrypt canonical `ready(N)`.
2. Return it without refresh when the access token meets the configured
   minimum TTL.
3. Otherwise CAS a credential-free `refresh_in_progress` recovery reservation as
   generation `N+1`. Only the process that proves its own reservation may
   dispatch OAuth; other processes fail closed.
4. Call the injected refresh adapter with the in-memory credential, reserved
   generation, and opaque attempt ID.
5. Validate exact credential shape, ChatGPT auth mode, JWT identity/plan/expiry
   claims, account and user continuity, changed access and refresh tokens, and
   the fixed authority safety TTL.
6. CAS the candidate or blocked outcome into generation `N+2`.
7. Re-read canonical encrypted state and require the exact generation, commit
   ID, and serialized payload before returning a token grant.

If key rotation advances the store generation after reservation while
preserving the exact reserved payload, the broker rebases the already-produced
outcome onto that newer generation without calling OAuth again. If rotation
happens after the outcome commit, an exact match of the outcome payload is also
safe to return. Each reservation has a unique owner ID. When its CAS has
already returned success, a higher-generation exact-payload successor preserves
proof that this broker owns that same reservation and may dispatch. A foreign
reservation in an ABA sequence has a different payload and remains fail-closed.
A different credential or blocked payload remains a conflict and is never
overwritten as a storage-only event.

A caller may request a TTL above the fixed authority safety TTL. A lower
per-call value is clamped to the authority floor and can never suppress a
required refresh. The
caller-specific requirement is checked only after the shared refresh completes;
it cannot mark otherwise valid canonical state as recovery-required. A caller
whose requirement is still unmet receives non-retryable
`token_ttl_insufficient` metadata without receiving the credential.

Ordinary `installCredential()` refuses to overwrite any recovery-fenced blocked
state, including terminal post-dispatch failures. Recovery is a separate
`recoverRefreshReservation()` operation that requires the exact canonical
generation and reservation owner ID. The credential-free `snapshot()` response
includes that non-secret owner ID for every fenced blocked state, so a trusted
supervisor can supply both recovery preconditions. Its caller must first stop
or fence the old broker owner; the library cannot prove external process death.
The replacement must preserve the source account and user identities while
rotating both the source access and refresh tokens. The encrypted reservation
hashes prevent an accidental identity switch or republication of the
potentially consumed credential.
This explicit takeover path preserves re-login recovery without allowing a
live refresh to acquire a second owner between reservation and dispatch.

The adapter must implement the verified `account/read` choreography inside a
per-refresh containment boundary. It may return `reauth_required` only from a
trusted structured observation. A trusted `preDispatch: true` failure restores
the prior ready credential through a new CAS generation before returning a
retryable error. Every unclassified adapter error is treated as post-dispatch
uncertainty and leaves or advances durable recovery state; error text is never
used to infer safety.

The MVP does not cancel a shared remote refresh because one worker disconnects.
No caller receives the candidate before durable CAS and canonical reread.

## Worker Gateway

At worker startup, the gateway initializes app-server with
`experimentalApi: true`, requests `workerLoginParams()`, and calls
`account/login/start`. On `account/chatgptAuthTokens/refresh`, it rejects
unknown fields, any reason other than `unauthorized`, and any previous account
ID that does not match canonical state.

One broker facade belongs to one worker connection and privately remembers both
the generation and access token last delivered by `workerLoginParams()` or a
refresh response. If a delayed `401` callback arrives after another connection
already committed a different access token for the same account, the facade
returns that canonical credential without rotating the refresh token again. A
generation change that preserves the rejected access token does not prove a
new access credential—it may be key rotation or an explicit reinstall—so it
still enters single-flight refresh. Caller-specific rejected-token checks also
prevent a current worker from being satisfied by a stale worker's no-op shared
task. It then returns exactly:

```json
{
  "accessToken": "<short-lived secret>",
  "chatgptAccountId": "<workspace ID>",
  "chatgptPlanType": "enterprise"
}
```

The response never includes the refresh token, ID token, raw auth JSON,
generation, key ID, or authority storage path. Workers never mount the broker
state directory and never persist `auth.json`.
The facade also privately binds callbacks to the account and user identities
actually issued by the preceding login response, so an old worker cannot claim
a newly installed identity and receive its access token.

## Failure and Recovery Rules

- Only a trusted structured pre-dispatch adapter failure may be reported
  retryable, and only after the old credential is restored in a newer durable
  generation.
- Structured permanent account loss moves canonical state to
  `reauth-required`; all grants remain blocked until credential installation
  commits a newer generation.
- A refresh reservation or any terminal blocked descendant cannot be replaced
  by ordinary login. Supervisor-fenced recovery must match its generation and
  unique owner ID before publishing replacement credentials.
- Any uncertain post-dispatch outcome, invalid refreshed credential, identity
  drift, unchanged access or refresh token, or failure to meet the fixed
  authority safety TTL
  moves canonical state to `recovery-required` and prevents reuse of the old
  refresh token. A higher caller-specific TTL never changes durable state.
- Clock reads must be finite numbers; thrown or non-finite readings fail closed
  before the broker returns a credential or owns a refresh reservation. The
  candidate TTL is checked again after the remote call using a fresh reading.
- `refresh_in_progress` is reserved for the credential-free durable reservation
  shape. The same reason from an adapter failure is normalized to
  `adapter_post_dispatch_uncertain` instead of producing malformed state.
- A stale CAS never returns an uncommitted candidate.
- Reservation acknowledgement reconciliation accepts an exact-payload
  successor only when its generation strictly advances beyond the attempted
  predecessor; raw equality without monotonic progress is not ownership proof.
- A post-dispatch CAS conflict may rebase only across generations whose exact
  broker payload still matches the durable reservation, or accept a later
  storage-only rotation whose exact payload matches the outcome.
- A lost commit acknowledgement is reconciled by canonical commit ID and
  payload replay using the same CAS attempt and a successful held-directory
  sync proof; it never triggers a second OAuth refresh automatically. If the
  exact durable commit cannot be proven, the result is non-retryable
  `refresh_outcome_uncertain`.
- Store recovery artifacts and integrity/configuration failures remain
  operator-recovery or invalid-state errors; they are not downgraded to
  retryable availability failures. A canonical symbolic link rejected by
  `O_NOFOLLOW` is specifically classified as invalid state, not transient I/O.
- Reservation conflict reconciliation preserves recovery and integrity error
  classes from its canonical reread. Lock release failure always takes
  precedence over a retryable operation error while retaining whether commit
  was impossible, completed, or uncertain. Broker mutation paths never retry a
  CAS after lock release failure, and external commit-state metadata is read as
  an allowlisted own data property. A primary error may preserve only
  `not-committed` or `uncertain`; definite `committed` state is derived only
  from a successful mutation outcome.
- Errors, snapshots, logs, and test evidence must not serialize tokens, raw
  auth JSON, JWT claims, or upstream error bodies.

## Security Boundary and Deferred Work

The MVP protects encrypted state at rest, authenticated integrity, torn writes,
same-process refresh storms, stale generation commits, authority/key mix-ups,
and common accidental serialization. It assumes the broker process, injected
key provider, authority directory, and supervisor singleton are trusted.

Explicitly deferred:

- multi-host leader election, fencing, and database-level CAS;
- KMS/HSM/TPM providers, key escrow, and rollback-resistant counters;
- NFS or object-store publication semantics;
- production rootless refresh containment and secret-classified stderr sinks;
- memory locking, core-dump/swap hardening, and secure deletion;
- broker RPC transport and worker-instance delivery audit persistence; and
- destructive live OAuth failure tests against anything except a disposable
  dedicated login.

The later production broker may replace the file store with a transactional
database without changing the broker's generation, state, or worker-delivery
contracts.
