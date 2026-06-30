# Project State

## Current State

- The repository contains the Codex review-gate template and external-auth
  compatibility probes for the portable runtime.
- Per-workstream implementation state lives under `docs/project_journal/`.

## Recovery Pointers

- External-auth probe workstream:
  `docs/project_journal/2026/06/2026-06-30-external-auth-probe-1424ea.md`

## Global Blockers

- The app-server `chatgptAuthTokens` integration is experimental and requires a
  pinned Codex binary or image plus compatibility testing on upgrades.
