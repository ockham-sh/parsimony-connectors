# Changelog — parsimony-destatis

All notable changes to `parsimony-destatis` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- BREAKING: `destatis_fetch` and `enumerate_destatis` migrated from the
  retired `/genesisWS/rest/2020/*` API to the public
  `/genesisGONLINE/api/rest/*` API. The legacy base now redirects all
  traffic to the announcement page; the connector was non-functional. No
  GAST/registered creds needed on the new base.
- `destatis_fetch` now parses JSON-stat 2.0 (was: ffcsv). The canonical
  parameter is `name=` (matches the new API's URL-path key);
  `table_id=` continues to work via a pydantic alias for backward
  compatibility. `username` / `password` are accepted as no-op kwargs so
  existing deployments don't have to drop them.
- `enumerate_destatis` now produces ~331 statistic + ~2,999 table rows
  with rich German "Qualitätsbericht" descriptions (was: 500-row
  truncated single-call against `/catalogue/tables`).
- `DESTATIS_ENUMERATE_OUTPUT` schema expanded to 11 columns including
  `description` (concat of subject area + DE/EN names + truncated
  German lead paragraph for statistics; table rows lift the parent
  statistic's description for retrieval signal). Mirrors BoJ.

### Added

- `destatis_search`: semantic search over the published Destatis catalog
  at `hf://parsimony-dev/destatis`. Override via
  `PARSIMONY_DESTATIS_CATALOG_URL`. Backed by
  `paraphrase-multilingual-MiniLM-L12-v2` (German-aware) so DE and EN
  queries hit the same entries.
- `CATALOGS = [("destatis", enumerate_destatis)]` for
  `parsimony.publish.publish_provider` discovery.

### Removed

- Legacy `/genesisWS/rest/2020/*` calls (API base retired upstream).
- 500-row catalog truncation.

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
