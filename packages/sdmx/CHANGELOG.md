# Changelog — parsimony-sdmx

All notable changes to `parsimony-sdmx` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `sdmx_dimension_search(agency, dataset_id, dimension, query=None)` — search or enumerate one
  DSD dimension's `(code, label)` values from the flow's series catalog. Absorbs the roles of the
  removed `sdmx_codelist_search` and the `refine` facet column.
- `sdmx_series_search` eagerly validates `filter_json` values against the flow's populated
  columns: a value the flow never populates raises `InvalidParameterError` naming the missing
  values and pointing at `sdmx_dimension_search`, instead of being silently dropped by `isin`
  (#48).
- `sdmx_series_search` empty-match autopsy: an all-AND filter that matches nothing reports
  per-column standalone counts, and when every column matches alone, a leave-one-out pass names
  the conflicting columns (#48).
- `sdmx_dimension_search` accepts `filter_json` (same syntax as `sdmx_series_search`) and scopes
  results to values populated *within* that slice, with the same eager validation and autopsy.
- `sdmx_fetch` surfaces `UNIT` / `UNIT_MULT` series attributes as labeled metadata columns;
  `value` is coerced numeric.
- `sdmx_fetch` verifies `'+'`-OR coverage: a requested code that contributed zero observations
  raises `EmptyDataError` naming the dimension and codes instead of silently vanishing.

### Changed

- Collapsed the agent surface to four connectors: `sdmx_datasets_search` → `sdmx_series_search` /
  `sdmx_dimension_search` → `sdmx_fetch`. Only published flows are searchable; an unpublished flow
  hard-errors ("not published; ask the maintainers to build it") with no live fallback.
- `sdmx_series_search` `filter_json` now accepts a bare scalar value as a single-code filter
  (`{"geo_code": "DE"}` == `{"geo_code": ["DE"]}`).

### Fixed

- `sdmx_fetch` classifies a no-data empty-document response (HTTP 200, empty body) as
  `EmptyDataError` with period-widening guidance, instead of a misleading
  "transient fetch error … Retry shortly" `ProviderError`.
- `sdmx_series_search` strips legacy flow-id prefixes from emitted keys so every `key` is
  fetch-ready, matching newer catalogs.

### Removed

- `sdmx_codelist_search` and `enumerate_sdmx_series` / `enumerate_sdmx_datasets` connectors, the
  `refine` facet column on `sdmx_series_search`, and standalone codelist-catalog building. DSD-level
  codelists are still resolved internally for title composition.

## [0.7.0]

### Changed

- **Adapted to `parsimony-core==0.7`**: Bump `parsimony-core` pin from `>=0.6.0,<0.7` to `>=0.7.0,<0.8`.
- **Dynamic hybrid catalogs**: operator builds choose BM25+vector `HybridIndex` per field when unique field text count is below 1,000, otherwise BM25-only; title and each SDMX dimension field on series catalogs, title/description on `sdmx_datasets` with `code` kept as a BM25 lookup index.
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
