## [Unreleased]

## [0.0.2] - 2026-08-10

### Changed

- Depends on the renamed `parsimony` distribution (was `parsimony-core`), pinned
  `~=0.0.1` (any 0.0.x kernel). The import name is unchanged. `parsimony-core` is
  discontinued, so 0.0.1 of this package no longer resolves — upgrade to 0.0.2.

### Breaking

- **`sdmx_fetch` takes `agency` + `dataset_id`** instead of composite `dataset_ref`.
  Flow identity matches `sdmx_series_search` / `sdmx_dimension_search`. `series_ref`
  is the catalog `key` (named `*_ref` so it does not trip the plugin `*_key` secrets
  check). `sdmx_datasets_search` emits `agency` and `dataset_id` columns — paste those
  into fetch and series search.
- **Search surface is literal `query=` + exact `filter=`.** There is no query grammar
  and no `filter_json` / `fields=` / `filters=` parameters. Rank with free text; pin
  with `filter=` (mapping shorthand, `F(...)`, or serializable expressions).
- **`sdmx_fetch` no longer strips a leading `{dataset_id}.` from `series_ref`.**
  Catalog keys are DSD-order only; build-time `_strip_flow_prefix` remains for ECB CSV
  ingest. Paste search `key` into `series_ref` as-is.

### Fixed

- **ECB series catalog keys are DSD-order only.** Search `key` pastes straight into
  `sdmx_fetch(series_ref=…)`.
- `sdmx_fetch` folds SDMX reporting-period notation in `TIME_PERIOD` onto the ISO
  forms the output schema already declares (`2023-M06` → `2023-06`, `2023-A1` → `2023`).
- `sdmx_fetch` classifies empty-document HTTP 200 responses as `EmptyDataError` with
  period-widening guidance.

### Added

- `sdmx_dimension_search(agency, dataset_id, dimension, query=None)` — search or
  enumerate one DSD dimension's `(code, label)` values from the flow's series catalog.
- Eager `filter=` validation and empty-match autopsy on series/dimension search.
- `sdmx_fetch` surfaces `UNIT` / `UNIT_MULT` as `UNIT_code` / `UNIT_label`; verifies
  `'+'`-OR coverage so requested codes that contribute zero observations raise
  `EmptyDataError`.

### Changed

- Ranking pair is now `score`, `search_detail` (typed JSON evidence from `parsimony-core`);
  the categorical `matched` column is removed.
- Hybrid catalog indexes build without a fusion config — fusion is native in core.
- `sdmx_series_search` `top_k_per_dim` default 5 → 50 (per-field scored-candidate cap).
- Series search ranks equal-weight across every indexed `{dim}_label` field via
  `Catalog.multi_field_search`. The composed `title` is display-only (not indexed);
  `field="title"` raises. Codes are for `filter=`, not free-text ranking.
- Collapsed agent surface: `sdmx_datasets_search` → `sdmx_series_search` /
  `sdmx_dimension_search` → `sdmx_fetch`. Unpublished flows hard-error.
- `sdmx_fetch` emits each dimension as `{dim}_code` (not a combined display string).

### Removed

- `build_series_catalog` no longer builds a `title` index (`title` column remains).
- `sdmx_codelist_search` and `enumerate_sdmx_series` / `enumerate_sdmx_datasets`, the
  `refine` facet column, and standalone codelist-catalog building.

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
