# Changelog — parsimony-destatis

All notable changes to `parsimony-destatis` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- `destatis_fetch` drops observations GENESIS flags as having no value. Upstream
  pads a not-yet-published or withheld cell with a literal `0.0` and marks it in the
  JSON-stat `status` array; passed through, a routine year-over-year calculation read
  that placeholder as a real -100% collapse. An exact zero (status `-`) is still
  returned, as are provisional/estimated/revised values. (#80)

## [0.8.0] — 2026-06-09

Full guidebook live-verification pass (catalog completeness + fetch coverage
proven against the live GENESIS-Online REST API, not just documented).

### Fixed

- **`destatis_fetch` no longer hard-fails ~25 % of tables.** The JSON-stat time
  axis was detected by dimension *name* (`ZEIT/JAHR/MONAT/QUARTAL`) with a
  fallback to dimension 0 — which is the constant `statistic` dimension. Any
  table whose time axis is named otherwise (`STAG`/`STAGV` reference dates,
  `SEMEST` semesters, `SMONAT`/`SQUART` ISO-8601-duration month/quarter,
  `SLJAHR` school years) emitted the *statistic code as a year* and raised
  `ParseError: year <code> is out of range`. The detector now identifies the
  time dimension by the **shape of its category keys** (the dimension whose keys
  parse as periods), never falling back to dimension 0, and normalises every
  period form — including ISO-8601 durations like `2015-05P1M` — to an ISO
  `YYYY-MM-DD` start. Live-verified: 12/12 sampled tables across all frequencies
  now fetch with real dates (was 9/12). The deceptive `MONAT`/`QUARTG`
  month-/quarter-of-year *classifications* (keys `MONAT10`/`QUART3`) are
  correctly **not** treated as the time axis.
- **`enumerate_destatis` no longer drops a tableless statistic.** A statistic
  whose `/tables` returns 404 and `/information` is empty (e.g. `61121`) is a
  legitimate "zero tables", not a fetch failure — its statistic row is now
  emitted from the index node rather than the statistic vanishing from the
  catalog entirely.

### Added

- `catalog_tests/queries.yaml` — curated code + title search probes for the
  recall gate (was missing).
- Offline tests for key-shape time-dimension detection (reference-date /
  ISO-duration / month-of-year-classification) and tableless-statistic
  survival.

### Verified (live, 2026-06-09)

- Universe: **3,009 tables across 331 statistics** (0 cross-statistic duplicate
  tables; max 132 tables/statistic; per-statistic `/tables` returns the full
  list with no pagination cap). No fetchable-but-unlisted table exists (enum
  gaps 404). The keyless REST API exposes **predefined tables only** — the
  cube/custom-table surface is absent on this host (all `/cubes`,`/data/cube`,
  `/metadata/*` paths 404) and out of scope by design.

## [0.5.0] — 2026-05-06
### Changed

- Adapted to `parsimony-core==0.5`. Connector code no longer constructs `Provenance` directly; the framework authors all provenance fields in `Connector._wrap_result`. Source-specific extras (where present) move to `Result.with_properties(**kwargs)`. Drops the `provenance=` and `params=` kwargs from `OutputConfig.build_table_result` / `Result.from_dataframe` call sites.
- Bump `parsimony-core` pin from `>=0.4.0,<0.5` to `>=0.5.0,<0.6` (and `[standard-onnx]` extra accordingly on catalog-publishing packages).
- BREAKING: `destatis_fetch` and `enumerate_destatis` migrated from the
  retired `/genesisWS/rest/2020/*` API to the public
  `/genesisGONLINE/api/rest/*` API. The legacy base now redirects all
  traffic to the announcement page; the connector was non-functional. No
  GAST/registered creds needed on the new base.
- `destatis_fetch` now parses JSON-stat 2.0 (was: ffcsv). The canonical
  parameter is `name=` (matches the new API's URL-path key);
  `table_id=` continues to work via a pydantic alias for backward
  compatibility. `username` / `password` are accepted as no-op kwargs so
  existing deployments don't have to drop them.
- `enumerate_destatis` now produces ~331 statistic + ~2,999 table rows
  with rich German "Qualitätsbericht" descriptions (was: 500-row
  truncated single-call against `/catalogue/tables`).
- `DESTATIS_ENUMERATE_OUTPUT` schema expanded to 11 columns including
  `description` (concat of subject area + DE/EN names + truncated
  German lead paragraph for statistics; table rows lift the parent
  statistic's description for retrieval signal). Mirrors BoJ.

### Added

- `destatis_search`: semantic search over the published Destatis catalog
  at `hf://parsimony-dev/destatis`. Override via
  `PARSIMONY_DESTATIS_CATALOG_URL`. Backed by
  `paraphrase-multilingual-MiniLM-L12-v2` (German-aware) so DE and EN
  queries hit the same entries.
- Provider-owned catalog build script for operator catalog refreshes.

### Removed

- Legacy `/genesisWS/rest/2020/*` calls (API base retired upstream).
- 500-row catalog truncation.

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
