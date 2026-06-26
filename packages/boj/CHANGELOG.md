# Changelog — parsimony-boj

All notable changes to `parsimony-boj` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-09

Re-run through the full connector guidebook process and **live-verified** against
the production BOJ Time-Series Data Search API (universe = **326,466 series**
across the 50 databases).

### Fixed

- **`boj_fetch` silent truncation (data loss).** `/getDataCode` caps each request
  at 250 series codes **and 60,000 data points** `(series × periods)`; over the
  point cap it returns HTTP 200 `"Successfully completed"` with only the first
  *K* series and a `NEXTPOSITION` cursor. The previous `boj_fetch` ignored
  `NEXTPOSITION`, so a multi-series request **silently dropped its tail** (22
  daily FX series → only 5 returned). It now paginates on `NEXTPOSITION`
  (`startPosition=…`), accumulating series across pages with a non-advancement
  guard, so the full result is always returned. Single-series fetches are
  unaffected (no BoJ series exceeds the point cap).

### Added

- **`scripts/harvest_databases.py`** — regenerates the frozen 50-DB registry from
  BoJ's machine-readable `api_tool.xlsx` `DB_Name` sheet (`--diff` checks for
  drift). The archetype-C "commit the harvester" discipline; the registry is
  cross-validated against both the manual §II.3.(2) and the XLSX (zero diff).
- `tests/test_public_surface.py` and a registry floor/shape test (pins `len == 50`,
  asserts the historical phantom `BP02` is absent).

### Changed

- **Package restructured** into `_http` / `outputs` / `databases` / `connectors/
  {fetch,enumerate}` / `search` / `catalog_build`; the monolithic 655-line
  `__init__.py` is now a thin facade. No change to the connector surface
  (`boj_fetch`, `enumerate_boj`, `boj_databases_search`, `boj_series_search`) or
  the multi-bundle catalog schema.
- DB titles/categories regenerated from the canonical `api_tool.xlsx` `DB_Name`
  sheet (e.g. FM01 now reads "…Call Rate (average)…").
- Completeness re-verified live: `getMetadata` is **uncapped** (returns every
  series per DB in one call, proven across all 50 — `CO`/TANKAN alone is 166,513
  series), so each per-DB series catalog is complete; every series is fetchable
  by `(db, code)`. The enumerate fan-out now parses + releases each DB payload as
  it arrives (the giant `CO` response is ~99 MB).

## [0.5.0] — 2026-05-06
### Changed

- Adapted to `parsimony-core==0.5`. Connector code no longer constructs `Provenance` directly; the framework authors all provenance fields in `Connector._wrap_result`. Source-specific extras (where present) move to `Result.with_properties(**kwargs)`. Drops the `provenance=` and `params=` kwargs from `OutputConfig.build_table_result` / `Result.from_dataframe` call sites.
- Bump `parsimony-core` pin from `>=0.4.0,<0.5` to `>=0.5.0,<0.6` (and `[standard-onnx]` extra accordingly on catalog-publishing packages).
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
- Provider-owned catalog build script for operator catalog refreshes.

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
