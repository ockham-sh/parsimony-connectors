# Changelog — parsimony-riksbank

All notable changes to `parsimony-riksbank` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-10

A deep-exploration pass against the Riksbank developer portal found that the connector
covered only **two of the Riksbank's five public REST APIs** (SWEA + SWESTR). This release
adds the missing three and restructures the package to the exemplar layout.

### Added

- **Monetary Policy Data** (`monetary_policy_data/v1`) — the forecasts & outcomes behind
  each Monetary Policy Report (~24 series across ~59 policy-round vintages). New
  `riksbank_monetary_policy_fetch(series, policy_round=...)`; omit the round to retrieve
  every vintage. The previous code excluded this family entirely, mislabelled "forecasts
  404s on every path" — it had simply probed the wrong base URL (the real path is
  `monetary_policy_data/v1/forecasts`, not `forecasts/v1`).
- **Turnover Statistics** (`turnover-statistics/v1`) — aggregated turnover on the Swedish
  fixed-income, FX and interest-rate-derivative markets (3 markets × 2 frequencies, full
  history since 1987). New `riksbank_turnover_fetch(market, frequency)`.
- **Holdings** (`holdings/v1`) — the Riksbank's holdings of Swedish securities
  (per-security and aggregated). New `riksbank_holdings_fetch(dataset, start_date=...)`.
  The provider advertises these as parquet files, but the data endpoint serves JSON by
  default — so no parquet/pyarrow dependency is needed.
- `scripts/build_catalog.py`, `tests/test_public_surface.py` and `tests/test_build_catalog.py`.

### Changed

- The catalog now spans all five products (~156 units: 117 SWEA + 7 SWESTR + 24 Monetary
  Policy + 6 Turnover + 2 Holdings). The enumerator KEY changed from `series_id` to a
  single routable `code` (SWEA/SWESTR keep bare ids; the three new families carry
  `monetary_policy/…`, `turnover/…`, `holdings/…` prefixes so a search hit routes to the
  right fetch verb). Added a `unit` metadata column; dropped the SWEA-only
  `frequency_source` heuristic tag.
- Restructured the 751-line `__init__.py` monolith into focused modules (`_http`, `swea`,
  `swestr`, `monetary_policy`, `turnover`, `holdings`, `outputs`, `connectors/*`,
  `search`, `catalog_build`) behind a thin facade.
- Bump `parsimony-core` pin to `>=0.7,<0.8`; version 0.7.0 → 0.8.0.

### Fixed

- **Monetary Policy colon-encoding (silent whole-universe fallback).** A policy-round name
  contains a colon (`2026:1`); httpx percent-encodes it to `%3A`, which the gateway 404s
  on — and a 404-then-retry-without-filter would have silently returned *all* series ×
  *all* vintages instead of the one requested. The fetch now builds the query with a
  literal colon (`safe=":"`), live-verified to return exactly the requested series/round.

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
