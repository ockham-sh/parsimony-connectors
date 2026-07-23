# Changelog — parsimony-bdf

All notable changes to `parsimony-bdf` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- The keyed verbs (`bdf_fetch`, `enumerate_bdf`) now declare
  `requires=("BDF_API_KEY",)` — the env var an agent must set for a call to
  succeed, surfaced in the connector card and named by the fast-fail
  `UnauthorizedError`. `bdf_search` is catalog-backed and keyless, so it declares
  nothing. Credential-declaration conformance tests added
  (`CredentialDeclarationSuite`) proving the declaration matches runtime.

## [0.8.0] — 2026-06-08

Ground-up refactor, run through the full connector guidebook process and
**live-verified** against the production Webstat API (the connector previously
shipped `UNVERIFIED-LIVE` because no key was on hand).

### Changed

- **Enumeration switched to archetype A (live full-index export).** The Webstat
  `series` dataset is a single flat queryable table, so `enumerate_bdf` now
  streams the entire ~41.6k-series universe in **one** `series/exports/json`
  call (plus one `webstat-datasets` call for the 45 dataflow stubs), replacing
  the previous 45-call per-dataset crawl. Completeness is self-tracking and
  verifiable by diffing `len(catalog)` against the live `series` total_count.
- **Bilingual, breadcrumb-rich catalog at no extra cost.** Series rows now carry
  English + French titles and the `path_en`/`path_fr` topic breadcrumb folded
  into the indexed `description`, improving cross-language and topical recall.
  No separate enrichment pass is needed (the source already serves both
  languages).
- **Package restructured** into `_http` / `outputs` / `connectors/{fetch,
  enumerate,_catalog}` / `search` / `catalog_build`, mirroring the `bde`
  exemplar; the top-level surface stays `CONNECTORS` + `load`.
- `bdf_fetch` validates `start_period` / `end_period` as ISO dates pre-network
  (`InvalidParameterError`) and tolerates null `obs_value` (missing-status gaps).

### Added

- Live integration suite (now runnable with `BDF_API_KEY`): keyed fetch, a
  dataset-bounded live enumerate, and a fixture-catalog search.
- `catalog_tests/queries.yaml` recall gate (referenced by the catalog-validate
  registry) and `tests/test_build_catalog.py` index-policy test.

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
