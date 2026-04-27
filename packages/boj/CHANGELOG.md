# Changelog — parsimony-boj

All notable changes to `parsimony-boj` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- `enumerate_boj` now uses the canonical 50-DB list from BoJ's official
  API manual (was 45 + phantom BP02). Adds Flow of Funds (FF), TANKAN
  (CO), BIS, Derivatives (DER), and Others (OT).
- `BOJ_ENUMERATE_OUTPUT` schema expanded to 13 columns including
  `description` (concat of breadcrumb + category + unit + frequency +
  db_title + notes), `entity_type`, and full coverage metadata. Mirrors
  BoC's enumerate schema.
- Added Akamai-aware throttling: concurrency cap 2, browser
  `User-Agent`, exponential backoff with `Retry-After` honoring on
  403/429/5xx, WARNING-level logging on failed DBs.

### Added

- `boj_search`: semantic search over the published BoJ catalog at
  `hf://parsimony-dev/boj`. Override via `PARSIMONY_BOJ_CATALOG_URL`.
- DB-level catalog rows with `db:<code>` keys for first-class DB
  discovery (mirrors BoC's `group:` pattern).
- `CATALOGS` module variable required by
  `parsimony.publish.publish_provider`.

### Fixed

- Removed phantom BP02 from the database list (not in BoJ's official
  manual).
- 403/429 responses are no longer silently swallowed; surfaced as
  warnings.

## [0.4.0] — 2026-04-24

Part of the first coordinated release of the
[`parsimony-connectors`](https://github.com/ockham-sh/parsimony-connectors)
monorepo under `parsimony-core==0.4`.

### Changed

- Connector rewritten against the kernel's `parsimony.discover` surface
  (`iter_providers`, `load`, `load_all`) and the `@connector(env=...)`
  decorator-level env-var declaration that replaced module-level
  `ENV_VARS`.
- Pin bumped to `parsimony-core>=0.4,<0.5`.
