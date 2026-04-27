# Changelog

Monorepo-level notable changes. Per-package changes live in each
`packages/<name>/CHANGELOG.md` — individual connectors version
independently on PyPI per [GOVERNANCE.md §5](GOVERNANCE.md#5-graduation-policy).

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

- Initial public release hygiene: `CODE_OF_CONDUCT.md`, `AGENTS.md`,
  root `CHANGELOG.md`, per-package `CHANGELOG.md`, `.github/ISSUE_TEMPLATE/`,
  `.github/dependabot.yml`.

### Changed

- `GOVERNANCE.md §6` simplified — structural rules (no provider-SDK
  copy-paste, no recorded cassettes, nominative trademark use) replace
  the provider-by-provider ToS-audit ceremony.

### Removed

- `docs/licence-audit.md` — the audit-tracker placeholder was dropped in
  favour of the structural rules in GOVERNANCE.md §6.

## [0.4.0] — 2026-04-24

First coordinated multi-connector release tracking `parsimony-core==0.4`.
Every `packages/<name>/` package publishes at this version under the new
kernel discovery surface (`parsimony.discover`, `@connector(env=...)`).
Per-package highlights live in each connector's own `CHANGELOG.md`.
