# Changelog — parsimony-treasury

All notable changes to `parsimony-treasury` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Credential-declaration conformance tests (`tests/test_credential_declaration_treasury.py`)
  prove the keyless connectors declare no requirements (`requires=()`, no `secrets=`)
  and reach the network with nothing configured.

### Changed

- `treasury_search` documentation states the discovery-catalog mechanism
  neutrally instead of overclaiming description-driven relevance.

## [0.8.0] — 2026-06-09

Re-run through the connector-guidebook process with deep, live exploration.

### Added

- `scripts/harvest_rate_feeds.py` — cross-validates the curated Office of Debt Management
  rate-feed registry against the live feed columns (the archetype-D "committed
  reproduction" discipline), plus a live integration cross-check. Both confirm the registry
  matches the current (2025) feed column union **exactly** — including the 1.5-month par
  point and the 6-week bill Treasury added in 2025.
- `catalog_tests/queries.yaml` (recall gate — was a dangling registry reference),
  `tests/test_public_surface.py`, `tests/test_build_catalog.py`.

### Fixed

- Renamed the catalog's prose column `definition` → **`description`** so it is actually
  indexed: `discovery_indexes` indexes `code`/`title`/`description`, so the rich Fiscal Data
  field text (named `definition`) was previously never searched — the catalog matched on
  `title` only. (The text still comes from Fiscal Data's `definition` field.)
- Dropped the dead **`RATE`** measure-type prefix — no Fiscal Data field is typed `RATE`
  (verified across all 2,987 fields).
- Fetch-time numeric coercion now uses the same prefix match as the enumerator
  (`CURRENCY*`/`NUMBER`/`PERCENTAGE*`) so a future precision-suffixed type on the fetch path
  is still coerced.

### Changed

- Restructured the monolithic `__init__.py` into the exemplar layout (`_http.py`,
  `parsing.py`, `rate_feeds.py`, `outputs.py`, `connectors/{__init__,fetch,enumerate}.py`,
  thin facade). No change to connector behaviour or wire shapes.
- Documented the deliberate scope boundaries: 3 static-file-only datasets (no JSON API) and
  the binary `.xls` HQM corporate-bond / Treasury-coupon-issue product (already in FRED).
- Bumped `parsimony-core` pin to `>=0.7,<0.8`.

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
