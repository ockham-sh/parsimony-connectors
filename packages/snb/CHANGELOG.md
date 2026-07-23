# Changelog — parsimony-snb

All notable changes to `parsimony-snb` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Credential-declaration conformance tests (`tests/test_credential_declaration_snb.py`)
  prove the keyless connectors declare no requirements (`requires=()`, no `secrets=`)
  and reach the network with nothing configured.

## [0.8.0] — 2026-06-09

Re-run through the connector guidebook process: the frozen cube registry was
replaced with live discovery, the data warehouse (previously excluded) was wrapped,
and the module was split to the exemplar layout.

### Added

- **The SNB data warehouse — 912 SDMX-style cubes** (groups BSTA / ZAST / ZAHL /
  DDUM / KRED / SNB1A / WKI), previously excluded entirely. `snb_fetch` now routes a
  warehouse cube id (`BSTA@SNB.AUR_U.ODF`) to `/api/warehouse/cube/{id}/data/csv/{lang}`
  with the id's `@` mapped to `.` — so every catalogued cube (1,149 total = 237
  publication + 912 warehouse) is fetchable through the one verb. Warehouse cubes are
  catalogued at cube level (their cartesian products are enormous; the leaves stay
  fetchable via `dim_sel`).
- **Live sitemap discovery (`enumerate_snb`).** The universe is now read from the
  published XML sitemap (`/sitemap`) at build time and self-tracks new cubes — no more
  frozen `_KNOWN_CUBES` registry. A committed `scripts/harvest_cubes.py` (the
  reproduction script the old comment promised but never shipped) re-derives and
  `--diff`s the universe.
- **Cube titles / units / frequency** are resolved from the portal's `getCubeInfo`
  endpoint (unlocked with the `x-epb-ajax` header; best-effort, with a synthesized
  fallback so a cube is never dropped for lack of a title) — replacing the hand-curated
  title map. `catalog_tests/queries.yaml` recall gate added.

### Changed

- Module restructured from a single ~845-line `__init__.py` to the exemplar layout
  (`_http`, `parsing`, `outputs`, `connectors/{fetch,enumerate}`, `search`,
  `catalog_build`, thin facade).
- Enumerate output gains a `unit` column and uses `getCubeInfo.publishingTitle` as the
  category (the sitemap topic/group label is the fallback).
- Bump `parsimony-core` pin to `>=0.7,<0.8`.

### Verified

- All connectors live-verified against the real portal: publication fetch (rendoblim
  monthly yields, devkum multi-currency FX), **warehouse fetch** (BSTA outstanding
  derivatives via the `@`→`.` route), the live sitemap universe (237 publication + 912
  warehouse), a bounded enumerate over one publication + one warehouse cube, and search
  over a fixture catalog. Keyless throughout (no secret can leak).

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
