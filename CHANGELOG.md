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
- **Ecosystem context updated across `README.md` and `docs/index.md`.**
  "Relation to the parsimony kernel" is replaced by "Relation to the
  parsimony ecosystem" — a three-repo table covering `parsimony`,
  `parsimony-connectors`, and `parsimony-agents` (now Apache-2.0,
  published independently on PyPI), plus a note on the fourth component
  `terminal` (AGPL-3.0 with commercial self-host licensing). This
  repo's own license and role are unchanged; the section expansion
  reflects the full OSS ecosystem becoming publicly visible at once.
- **`GOVERNANCE.md` revision-trigger footnote** no longer references
  `DESIGN-distribution-model.md §11` (an internal planning file not
  present in this repo). The trigger is now stated in terms of
  observable events: kernel-contract or connector-model changes.
- **`packages/sec_edgar/README.md`** corrects the PyPI distribution name
  from `parsimony-sec_edgar` to `parsimony-sec-edgar` (hyphen, not
  underscore, matching the published package name).
- **`packages/sdmx/README.md`** replaces a hardcoded machine-specific
  path (`/home/espinet/ockham/parsimony-connectors`) in the catalog-
  publishing instructions with the generic placeholder
  `<path-to-parsimony-connectors>`.

### Removed

- `docs/licence-audit.md` — the audit-tracker placeholder was dropped in
  favour of the structural rules in GOVERNANCE.md §6.

## [0.4.0] — 2026-04-24

First coordinated multi-connector release tracking `parsimony-core==0.4`.
Every `packages/<name>/` package publishes at this version under the new
kernel discovery surface (`parsimony.discover`, `@connector(env=...)`).
Per-package highlights live in each connector's own `CHANGELOG.md`.
