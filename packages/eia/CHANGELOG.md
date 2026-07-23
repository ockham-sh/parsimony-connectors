# Changelog — parsimony-eia

All notable changes to `parsimony-eia` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- The keyed verbs (`eia_fetch`, `eia_fetch_series`, `eia_facets`,
  `enumerate_eia`) now declare `requires=("EIA_API_KEY",)` — the env var an agent
  must set for a call to succeed, surfaced in the connector card and named by the
  fast-fail `UnauthorizedError`. `eia_search` is catalog-backed and keyless, so
  it declares nothing. Credential-declaration conformance tests added
  (`CredentialDeclarationSuite`) proving the declaration matches runtime.

## [0.8.0] — 2026-06-09

Re-run through the connector guidebook process: documentation compiled,
completeness proven live, and the shallow top-level-routes enumerator replaced
with a real searchable catalog.

### Fixed

- **Silent pagination truncation (data loss).** EIA caps every `/data` response
  at 5,000 rows; `eia_fetch` previously made a single call and returned only the
  first 5,000 of whatever matched (e.g. 5,000 of `petroleum/pri/spt` daily's
  91,285 rows). Both fetch verbs now read `response.total` and page through with
  `offset` until complete, guarded by a row-count ceiling that raises an
  actionable `InvalidParameterError` (echoing EIA's own "constrain with facet,
  start, end" guidance) instead of either truncating or pulling millions of rows.
- **400 errors lost their message.** A bad measure/frequency/facet returns a
  clean HTTP 400 with a useful JSON body; it now maps to a message-preserving
  `InvalidParameterError` rather than a generic `ProviderError(400)`.
- Period parsing now covers every EIA frequency (annual, monthly, daily, weekly,
  quarterly `YYYY-Q#`, hourly `…THH`, local-hourly with a TZ band).

### Added

- **A searchable dataset catalog (`eia_search` + `enumerate_eia` rewrite).** The
  enumerator now walks the full v2 route tree to one row per leaf dataset (232 at
  release), each carrying its measure + facet manifest folded into the indexed
  description. `eia_search` resolves a query to a dataset route. `catalog_build`,
  `scripts/build_catalog.py`, and `catalog_tests/queries.yaml` added.
- **`eia_fetch_series`** — fetch by a legacy APIv1 series id (`PET.RWTC.D`,
  `ELEC.SALES.CO-RES.A`) via the `/v2/seriesid/{id}` path, the addressing scheme
  used across the EIA/FRED ecosystem. This path lives outside the route tree, so
  it is the only way to reach a famous series straight from its id.
- **`eia_facets`** — list a facet dimension's `{id, name}` values so a fetch can
  be narrowed to a specific series (essential for huge datasets — electricity
  hourly is ~18.7M rows).
- **Facet filtering on `eia_fetch`** via a `facets={id: value|[values]}` param.

### Verified

- Catalog covers all **232 leaf datasets** across 14 categories (0 duplicate
  codes; exact match to an independent route-tree walk). Every series is fetchable
  by route+facets or legacy series id (EIA's ~2M-series universe is the facet
  cartesian product — uncatalogable, fully accessible). Bump to
  `parsimony-core>=0.7,<0.8`.

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
