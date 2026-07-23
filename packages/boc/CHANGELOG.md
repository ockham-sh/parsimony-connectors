# Changelog — parsimony-boc

All notable changes to `parsimony-boc` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Credential-declaration conformance tests (`tests/test_credential_declaration_boc.py`)
  prove the keyless connectors declare no requirements (`requires=()`, no `secrets=`)
  and reach the network with nothing configured.

### Fixed

- `boc_fetch` raises `EmptyDataError` for a series or group name Valet does not
  recognise (upstream 404), matching BLS and SEC EDGAR. It previously surfaced a raw
  `ProviderError`, which a generic `except EmptyDataError` did not catch. Real
  upstream failures still raise `ProviderError`. (#77)

## [0.8.0] — 2026-06-09

Re-run through the full connector guidebook process and **live-verified** against
the production Valet API and bulk list endpoints.

### Added

- **Observations URL-length guard.** Valet 302-redirects any `/observations`
  request whose URL exceeds ~4096 bytes (a limit on URL length, not series
  count). `boc_fetch` now rejects oversized requests pre-network with an
  actionable `InvalidParameterError` (split the names, or fetch a whole panel
  with `group:NAME`) instead of an opaque redirect surfacing as a `ParseError`.
- **Retired-group pruning.** The per-group membership fan-out doubles as a
  liveness probe: ~29 dated one-off panels that 404 on both `/groups/{name}` and
  `/observations/group/{name}` are pruned from the catalog so it never offers an
  unfetchable panel. A *transient* (5xx/network) failure keeps the group
  best-effort — only a definitive 404 prunes.
- A `failed/total` enumeration summary log (series + live groups + pruned +
  transient failures), `catalog_tests/queries.yaml` (recall gate), and
  `tests/test_public_surface.py`.

### Changed

- **Package restructured** into `_http` / `outputs` / `connectors/{fetch,
  enumerate,__init__}` / `search` / `catalog_build`; the monolithic
  `__init__.py` is now a thin facade. No change to the connector surface
  (`boc_fetch`, `enumerate_boc`, `boc_search`) or the catalog schema.
- Completeness re-verified live: `/lists/series/json` (15,638 series) is the
  authoritative universe — a full fan-out over all 2,441 groups surfaces **0**
  members absent from it; ~99.7% of listed series are fetchable (stale entries
  return a clean `EmptyDataError`); every live group panel is fetchable.

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
