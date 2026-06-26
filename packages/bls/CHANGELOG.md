# Changelog — parsimony-bls

All notable changes to `parsimony-bls` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-09

Ground-up refactor, run through the full connector guidebook process and
**live-verified** against the production BLS API and bulk flat-file site.

### Added

- **`enumerate_bls_surveys`** (tier-1 feed) + **`bls_surveys_search`** — a complete
  catalog of every BLS survey, each carrying a `dimensions` manifest (codes +
  labels) for series-id construction.
- **`enumerate_bls_series`** (tier-2 feed) + **`bls_series_search`** — per-survey
  series catalogs read from the authoritative `download.bls.gov/pub/time.series/
  <survey>/<survey>.series` flat files, with lexical title and **structured
  dimension** (`FIELD: value`) search. Built for the headline economic surveys and
  lazy-buildable on demand; large catalogs are loaded from a published snapshot or
  built and LRU-cached.
- `scripts/build_catalog.py` (two-tier publish), `catalog_tests/queries.yaml`
  recall gate, and `tests/test_build_catalog.py` / `tests/test_flatfiles.py`.
- `curl_cffi` as a **hard dependency** — the flat-file host is Akamai bot-managed
  and only a Chrome TLS handshake passes (the data API host stays plain httpx).

### Changed

- **Discovery is now two-tier, mirroring `parsimony-sdmx`.** The old enumerator
  crawled only `timeseries/popular` (~top series per survey) — a shallow, never-
  complete catalog. The universe is ~tens of millions of series (15.6 GB of
  `.series` metadata) and cannot be embedded whole; the new design catalogs
  surveys + dimension vocabularies completely and per-survey series for the
  headline surveys, while **every** series remains fetchable by id.
- Package restructured into `_http` / `surveys` / `flatfiles` / `_titles` /
  `outputs` / `catalog_policy` / `catalog_build` / `connectors/{fetch,
  enumerate_surveys,enumerate_series,search}`; the monolithic `__init__.py` is gone.
- `bls_fetch` tolerates suppressed (`-`) observation values and `S03` (semiannual
  annual-average) periods; titles compose from dimension labels for the surveys
  whose `.series` file lacks a `series_title` (SM/JT/PR).

### Removed

- `enumerate_bls` (the shallow `timeseries/popular` crawl), superseded by the
  flat-file-backed two-tier enumeration.

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
