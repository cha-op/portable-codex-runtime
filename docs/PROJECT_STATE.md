# Project State

## Current State

- The repository contains the Codex review-gate template and external-auth
  compatibility probes for the portable runtime.
- A managed authority can proactively refresh ChatGPT credentials through
  `account/read` without a model turn, then atomically promote verified state.
- Per-workstream implementation state lives under `docs/project_journal/`.

## Recovery Pointers

- Runtime delivery plan:
  `docs/project_journal/2026/07/2026-07-01-runtime-delivery-plan-6f13a8.md`
- Auth refresh authority spike:
  `docs/project_journal/2026/07/2026-07-01-auth-refresh-authority-8b2e41.md`
- External-auth probe workstream:
  `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`

## Global Blockers

- The app-server `chatgptAuthTokens` integration is experimental and requires a
  pinned Codex binary or image plus compatibility testing on upgrades.
