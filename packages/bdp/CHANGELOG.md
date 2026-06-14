# Changelog — parsimony-bdp

All notable changes to `parsimony-bdp` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-09

Ground-up refactor, run through the full connector guidebook process and
**live-verified** against the production BPstat API (keyless).

### Fixed

- **Completeness: the datasets list is now paginated.** `/domains/{id}/datasets/`
  caps at 10 items per page; the previous enumerator read only page 1, so the 3
  domains with more than 10 datasets (domain 19 has 25) silently lost datasets —
  and every series in them. The crawl now follows `extension.next_page`
  (`page_size=100`). Verified: domain 19 recovers all 25 datasets / 4,094 series,
  domain 1 recovers all 16,644 series — both exact matches to the declared counts.

### Changed

- **Leaner per-dataset crawl.** The dataset-detail crawl now uses
  `page_size=100&obs_last_n=1` (100 series/page with a one-point observation
  array, ~70 KB) instead of the default 10 series/page with full observation
  history. This cuts the universe crawl from ~7,200 pages to ~720 and removes the
  502s a naive `page_size=100` triggered on large datasets.
- **Bilingual catalog via a `/series/` enrichment pass.** The crawl now only
  discovers ids + terse English labels; the rich, search-bearing descriptions
  come from a build-time `/series/?series_ids=` pass (100 ids/call) in **English
  and Portuguese**. English is the primary search signal; Portuguese folds into
  the indexed `description` for cross-language recall on the BM25 index. Batched,
  retried, split-on-failure.
- **Package restructured** into `_http` / `outputs` / `enrich` / `connectors/
  {fetch,enumerate,_catalog}` / `search` / `catalog_build`, mirroring the `bde`
  exemplar; the top-level surface stays `CONNECTORS`. The monolithic 843-line
  `__init__.py` is gone.
- **Enumerator schema simplified.** Dropped the fragile JSON-stat-dimension-derived
  `frequency` / `units` / `start_date` / `end_date` columns (frequency is a
  per-series dimension, not a dataset property; the frequency/unit words now ride
  in the prose `description` in both languages). Added `short_label` and a
  per-dataset / per-domain `num_series`.
- `bdp_fetch` validates `start_date` / `end_date` as ISO dates pre-network
  (`InvalidParameterError`) and tolerates null observation values.
- The enumerator self-checks each crawled dataset against its declared
  `num_series` and logs any shortfall.

### Added

- `catalog_tests/queries.yaml` recall gate (exact `code:` + lexical title probes)
  and `tests/test_build_catalog.py` index-policy test.

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
