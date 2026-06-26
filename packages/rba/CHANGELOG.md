# Changelog — parsimony-rba

All notable changes to `parsimony-rba` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-09

Re-run through the connector guidebook process: documentation compiled (including
the canonical `readrba` R client), completeness proven live, the monolithic module
split to the exemplar layout, and the catalog↔connector gap closed.

### Fixed

- **Catalogued-but-unfetchable series (the "catalog ⊋ connector" gap).** The
  enumerator catalogued XLSX-exclusive (`rba_xlsx`, the Bond Purchase Program) and
  legacy xls-hist (`rba_xlsx_hist`, ~480 discontinued-series rows) entries, but
  `rba_fetch` could resolve **only CSV stems** — so those ~487 series were
  discoverable yet not fetchable. `rba_fetch` now resolves a `table_id` across all
  three publication formats (CSV stem, `workbook/sheet` for current XLSX-exclusive
  sheets, and `<stem>`/`<stem>/<sheet>` for xls-hist), so **every catalogued series
  is fetchable**.

### Added

- **Dynamic XLSX exclusivity.** The current-XLSX pass no longer relies on a hardcoded
  `{a03: Bond Purchase Program}` sheet allow-list; it emits a workbook series only if
  its id is not already covered by the CSV pass. Self-maintaining (a future XLSX-only
  sheet is picked up automatically) and live-proven to yield exactly the 7 Bond
  Purchase Program series.
- `catalog_tests/queries.yaml` (recall gate — fixes a dangling registry reference),
  `tests/test_public_surface.py`, and `tests/test_build_catalog.py`. Live offline +
  integration tests for the new XLSX-exclusive and xls-hist fetch paths.

### Changed

- Module restructured from a single ~900-line `__init__.py` to the exemplar layout
  (`_http`, `parsing`, `outputs`, `connectors/{fetch,enumerate}`, `search`,
  `catalog_build`) with a thin facade. No behaviour change to the curl_cffi/Akamai
  transport. Bump to `parsimony-core>=0.7,<0.8`.

### Verified

- Catalog covers **4,672 entries** (0 duplicate codes) across all 9 categories:
  rba_csv 4,186 + rba_xlsx 7 + rba_xlsx_hist 479; `required_recall` 1.00. An audit
  proved the skipped layers (the ~70 tables-page `*hist.xlsx` and the 11 period-range
  archives) add zero new series (same ids, longer history only). Universe cross-checked
  against the `readrba` R client (~4,354 incl. forecasts). All three formats
  live-fetched (cash-rate CSV, Bond Purchase Program XLSX, `b03hist` xls-hist).

## [0.5.0] — 2026-05-06

### Changed

- Adapted to `parsimony-core==0.5`. Connector code no longer constructs `Provenance` directly; the framework authors all provenance fields in `Connector._wrap_result`. Source-specific extras (where present) move to `Result.with_properties(**kwargs)`. Drops the `provenance=` and `params=` kwargs from `OutputConfig.build_table_result` / `Result.from_dataframe` call sites.
- Bump `parsimony-core` pin from `>=0.4.0,<0.5` to `>=0.5.0,<0.6` (and `[standard-onnx]` extra accordingly on catalog-publishing packages).
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
