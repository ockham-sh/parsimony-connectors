# Changelog — parsimony-sdmx

All notable changes to `parsimony-sdmx` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Removed

- **`build_series_catalog` no longer builds a `title` index.** `compose_series_title`
  concatenates the same dimension labels the `{dim}_label` indexes already carry, so the
  index held no information they lacked while costing one `__title__N` pseudo-member
  entity per distinct title (59,498 on a 60,691-row catalog, near 1:1 with rows).
  Measured over the 41-case search battery, removing it *improved* ranking
  (MRR 0.725 → 0.739; both adversarial "youth unemployment" cases moved to rank 1),
  because the composed title was double-counting terms the labels already matched.
  `TITLE_INDEX_MAX_VALUES` — a silent coverage cap on catalogs above 100k distinct
  titles — is removed with it.

  `title` remains a parquet column and the `TITLE` output column; only its index is
  gone. Scoping a query to it is now an error rather than a silent miss:
  `sdmx_series_search(fields="title")` raises `InvalidParameterError`, and `title:` is
  no longer parsed as a query clause (it falls through to the bare-query surface). Use
  a bare query to rank against every dimension label, or scope to a `{dim}_label`.
  Filtering on `title` via `filter_json` still works — that is a parquet operation and
  needs no index.

  **No republish is required.** This is a build-side change, and already-published
  catalogs keep serving correctly under it — a query is only ever scored against
  dimension-label fields, so their title index goes unused and its `__title__N`
  members never surface as results (covered by a regression test that builds a
  catalog the old way and drives it through the connector). Each catalog sheds the
  index whenever it is next built for any reason; measured on a 3,600-series flow
  that is a 51% cut in published size, so the ~7.9k catalogs get smaller as they
  turn over rather than needing a coordinated rebuild.

  One behaviour does change immediately for old snapshots: `fields="title"` is
  refused even where the index is still present, so the answer does not depend on
  which vintage of catalog happens to be cached. (#66)

### Fixed

- `sdmx_fetch` folds SDMX reporting-period notation in `TIME_PERIOD` onto the ISO
  forms the output schema already declares: `2023-M06` -> `2023-06`, `2023-A1` ->
  `2023`. IMF_DATA sends the reporting-period spelling where ECB and Eurostat send
  ISO, and `pd.to_datetime("2023-M06")` does not raise — it silently returns
  `2023-01-01 00:00:06`, corrupting sort order and any date axis built from the
  column. Notations with no ISO equivalent (`S`/`T`/`W`) pass through unchanged and
  stay loudly unparseable. (#79)

### Added

- `matched` output column on all three search connectors — which evidence
  surfaced the row (`lexical` / `semantic` / `both`). An all-`semantic` result page means nothing
  lexically real matched: rephrase the query rather than trust the order.
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
- `sdmx_fetch` surfaces `UNIT` / `UNIT_MULT` series attributes as split
  `UNIT_code` / `UNIT_label` metadata columns; `value` is coerced numeric.
- `sdmx_fetch` verifies `'+'`-OR coverage: a requested code that contributed zero observations
  raises `EmptyDataError` naming the dimension and codes instead of silently vanishing.

### Changed

- Hybrid catalog indexes are built without a fusion config — fusion is computed natively in core
  now. Requires the parsimony-core release carrying the single-path search; the `parsimony-core`
  pin is floor-bumped at release.
- `sdmx_series_search` `top_k_per_dim` default 5 → 50, matching the per-field scored-candidate
  cap the ranking battery validated; its description now states honestly that it bounds scored
  candidates per field, not returned results.
- All three search connectors (`sdmx_series_search`, `sdmx_datasets_search`,
  `sdmx_dimension_search`) now end with the same ranking trio — `coverage`, `score`,
  `matched` — with identical meanings (core's shared column definitions). What varies per
  surface is the distribution of values, never the schema: coverage is the ranking's first
  key on the series facet surface, and on the datasets title surface it is mostly 0.0 with
  an exact-title hit reading 1.0 — which now visibly explains why a pinned row outranks
  higher scores. `sdmx_dimension_search` enumeration reads carry the trio as nulls. The
  datasets unscoped fan-out merges exact-title pins first, then per-agency rank, then
  score; the broken `code: AGENCY|FLOW` hint is removed.
- `sdmx_series_search` bare queries now score across every indexed dimension-label
  field (`Catalog.search(fields=...)`), ranked by (`coverage` desc, `score` desc):
  `coverage` is the fraction of the query's words literally consumed by the row's
  dimension labels (1.0 = the query names the slice exactly), `score` is honest fuzzy
  relevance. The composed `title` is a display column, not a search surface — it
  concatenates the very labels the label indexes already carry, so scoring it only
  re-counted matched terms; on the 41-case ranking battery, keeping it off the surface
  raises the leading config MRR 0.725 → 0.739 and moves both adversarial phrasing
  cases ("monthly youth unemployment germany") to rank 1. The trade-off: a bare
  single-concept query ("swiss franc") no longer inherits title word order to prefer
  numerator over denominator series — both readings surface. Published catalogs are
  unchanged (the title index is simply not queried; its removal from builds is a
  future recipe change). Code fields stay out of the surface — codes remain exact
  identifiers for `filter_json`. Requires the parsimony-core release carrying
  `fields=` + coverage ranking.
- **BREAKING — `sdmx_series_search` renames `field=` to `fields=`**, accepting one
  indexed field name (old behavior) or a list to fuse a declared subset, mirroring
  `Catalog.search`.
- `sdmx_datasets_search` now searches flow descriptions alongside titles
  (`fields=["title", "description"]` when the catalog indexes description) and
  emits the same `coverage` column; cross-agency merge sorts by (coverage, score).
- `sdmx_dimension_search` ranked queries order values by (coverage, score): a value
  the query names exactly ranks first, false-friend neighbors directly below.
- Collapsed the agent surface to four connectors: `sdmx_datasets_search` → `sdmx_series_search` /
  `sdmx_dimension_search` → `sdmx_fetch`. Only published flows are searchable; an unpublished flow
  hard-errors ("not published; ask the maintainers to build it") with no live fallback.
- `sdmx_series_search` `filter_json` now accepts a bare scalar value as a single-code filter
  (`{"geo_code": "DE"}` == `{"geo_code": ["DE"]}`).
- **`sdmx_fetch` emits each dimension as a bare `{dim}_code` column** (e.g. `FREQ_code="M"`)
  instead of a combined `"M (Monthly)"` display string, so the code is usable directly for
  filter/groupby/re-fetch and lines up with `sdmx_series_search`'s code fields. The human labels
  already ride in `title`, so a per-dimension label column would only restate them; `UNIT` /
  `UNIT_MULT` keep a `_label` since their meaning qualifies `value` and is not in the title.
  Column *casing* stays provider-inherited (ESTAT lowercase, ECB uppercase) (#46).

### Fixed

- `sdmx_series_search` no longer discards `query=` when `filter_json=` is given without
  `fields=` — the query now ranks rows within the filtered slice.
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
