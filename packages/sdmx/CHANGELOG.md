# Changelog — parsimony-sdmx

All notable changes to `parsimony-sdmx` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.7.0]

### Changed

- **Adapted to `parsimony-core==0.7`**: Bump `parsimony-core` pin from `>=0.6.0,<0.7` to `>=0.7.0,<0.8`.
- **Dynamic hybrid catalogs**: operator builds choose BM25+vector `HybridIndex` per field when unique field text count is below 100k, otherwise BM25-only; title and each SDMX dimension field on series catalogs, title/description on `sdmx_datasets` with `code` kept as a BM25 lookup index.
- **Unified Catalog Loading**: Updated catalog search connectors to use `Catalog.load` instead of `Catalog.from_url` or custom caching.
- **Unified Catalog Saving**: Updated catalog build script to call `Catalog.save` instead of `Catalog.push`.
- **Local LRU**: `sdmx_series_search` / `sdmx_datasets_search` now own their per-namespace catalog LRU (previously delegated to the kernel). `PARSIMONY_SDMX_CATALOG_LRU_SIZE` env var still configures it.

## [0.5.0] — 2026-05-06
### Changed

- Adapted to `parsimony-core==0.5`. Connector code no longer constructs `Provenance` directly; the framework authors all provenance fields in `Connector._wrap_result`. Source-specific extras (where present) move to `Result.with_properties(**kwargs)`. Drops the `provenance=` and `params=` kwargs from `OutputConfig.build_table_result` / `Result.from_dataframe` call sites.
- Bump `parsimony-core` pin from `>=0.4.0,<0.5` to `>=0.5.0,<0.6` (and `[standard-onnx]` extra accordingly on catalog-publishing packages).
- `sdmx_fetch` no longer delegates to a `_legacy_sdmx` shim; the body
  is inlined in `connectors/fetch.py` and reuses the existing
  `core/codelists`, `providers/sdmx_extract`, and `providers/sdmx_flow`
  pipeline. Behaviour is identical for the live observation table.
- `providers/sdmx_client.sdmx_client()` accepts `wb_url_rewrite=False`
  to install the `dataapi.worldbank.org` → `api.worldbank.org` host
  rewrite when the live fetch path needs it. Default is unchanged for
  every existing caller.

### Added

- `core/titles.compose_observation_title` and
  `core/titles.format_code_with_label`, sibling helpers to
  `compose_series_title` for the per-observation result schema.
- `providers/dataset_urls.build_sdmx_dataset_url` for agency portal URLs.
- Test coverage for `sdmx_fetch`, `dataset_urls`, and the
  `wb_url_rewrite` option.

### Removed

- `parsimony_sdmx/_legacy_sdmx.py` and its five never-wired connectors
  (`sdmx_list_datasets`, `sdmx_dsd`, `sdmx_codelist`, `sdmx_series_keys`,
  `enumerate_sdmx_dataset_codelists`) plus their param classes.

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
