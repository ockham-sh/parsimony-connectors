# Changelog — parsimony-sdmx

All notable changes to `parsimony-sdmx` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

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
