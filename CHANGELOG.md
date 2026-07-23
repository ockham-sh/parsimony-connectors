# Changelog

Monorepo-level notable changes. Per-package changes live in each
`packages/<name>/CHANGELOG.md` — individual connectors version
independently on PyPI per [GOVERNANCE.md §5](GOVERNANCE.md#5-graduation-policy).

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Removed

- **SDMX series catalogs no longer build a `title` index.** The composed title is the
  flow's dimension labels concatenated, so indexing it restated the `{dim}_label`
  indexes while costing one `__title__N` pseudo-member per distinct title — 59,498 of
  them on a 60,691-row catalog. Dropping it also scored better on the 41-case search
  battery (MRR 0.725 → 0.739). Title stays a display column served off the parquet;
  `fields="title"` and `title:` now raise `InvalidParameterError` naming the reason,
  and `TITLE_INDEX_MAX_VALUES` (a silent coverage cap) is gone. No republish is
  required: already-published catalogs keep serving correctly (their title index simply
  goes unused, since a query is only ever scored against dimension-label fields), and
  each one sheds the index the next time it is built for any reason — worth about half
  its published size. (#66)

### Fixed

- **Workspace**: the dev-only `parsimony-core = { path = "../parsimony" }` override
  leaked into external resolution, so
  `uv add "parsimony-<x> @ git+…#subdirectory=packages/<x>"` failed with
  "has no subdirectory `../parsimony`". The out-of-checkout sources now live in a
  `workbench` dependency group and are scoped to it; a single connector installs
  from git again. Note: `uv sync --no-dev` now resolves the kernel from PyPI. (#71)
- **`destatis_fetch`**: GENESIS pads a not-yet-published or withheld cell with a
  literal `0.0` and flags it in the JSON-stat `status` array. Those cells are now
  dropped instead of passed through, where a routine year-over-year calculation read
  them as a real -100% collapse. An exact zero (status `-`) is still returned. (#80)
- **`sdmx_fetch`**: `TIME_PERIOD` values in SDMX reporting-period notation
  (`2023-M06`, `2023-A1` — IMF_DATA) are folded onto the ISO forms the schema already
  declares. `pd.to_datetime("2023-M06")` does not raise; it silently yields
  `2023-01-01 00:00:06`. Notations with no ISO equivalent (`S`/`T`/`W`) pass through
  unchanged. (#79)
- **`boc_fetch`**: an identifier Bank of Canada does not recognise now raises
  `EmptyDataError`, matching BLS and SEC EDGAR, instead of a raw `ProviderError` that
  a generic `except EmptyDataError` missed. Real upstream failures stay
  `ProviderError`. (#77)
- **`fmp_prices`** and the three `fmp_*_statements` verbs return rows oldest-first,
  the ordering every other time-series connector uses, so `.iloc[-1]` is the latest
  observation. (#74)
- **`rba`**: a connection refused by RBA's bot-mitigation edge no longer reports
  "upstream timed out" (it fails in well under a second, which read as an
  undocumented client-side cooldown — there is none). The error now names what
  happened, and `enumerate_rba` documents that a heavy crawl can trip the edge. (#76)

### Changed

- **`fred_search`** takes `query=` (was `search_text=`), and
  **`alpha_vantage_search`** takes `query=` (was `keywords=`) — `query` is now the
  free-text parameter on every search connector, including
  `polymarket_search_events`. **Breaking** for callers passing the old names. (#72)
- **FMP plan tags corrected against FMP's own plan matrix and a live key.** Every
  verb's tag was re-derived and re-verified; 11 of 19 were wrong, including two the
  field report caught (`fmp_index_constituents` was `[All plans]` but needs Premium;
  `fmp_analyst_estimates` was `[Professional+]` but works on the free plan). The old
  tags named a `[Professional+]` plan FMP does not sell — the ladder is Basic (free) →
  Starter → Premium → Ultimate, and the tags now use it. The "unpredictable gating"
  in the report was our labelling, not FMP's: the two non-endpoint gates are a
  fixed 87-ticker sample on Basic and a per-plan date-range cap, both now documented
  and both still surfacing as `PaymentRequiredError`. A test pins the tag table. (#73)
- **Docs**: `sec_edgar_income_statement` warns that filers tag total revenue under
  different XBRL concepts (#82); FMP's `eps` and Alpha Vantage's TTM `EPS` state their
  period conventions (#78); every search connector says whether it reads a catalog
  snapshot (keyless) or makes a keyed live call (#75).

## [0.7.0]

### Changed

- **Catalog-backed connectors** now depend on `parsimony-core[catalog]>=0.7,<0.8`
  (all 11 search-capable macro connectors pull the full hybrid catalog stack).
- **Non-catalog connectors** pin plain `parsimony-core>=0.7,<0.8`.
- **BLS / Riksbank**: optional `BLS_API_KEY` / `RIKSBANK_API_KEY` env fallback
  (quota boosters; keyless when absent).
- **Riksbank search**: surfaces `source` column for SWEA/SWESTR routing.
- **Polymarket**: preserves `clobTokenIds` in market listings for discovery→price.
- **SDMX**: agencies corrected to ECB, Eurostat, IMF, World Bank; ESTAT macro
  recall fix in `series_selection.py`; remote catalog tests expect schema v1.
- **Catalog URL override**: standardized `load(catalog_url=...)` / env override
  convention across catalog-backed connectors.
- **Docs**: auth/discovery matrix, refreshed catalog manifest/operations, roster
  counts include enumerators and factory search connectors.

## [0.6.0-pre]

### Added

- Initial public release hygiene: `CODE_OF_CONDUCT.md`, `AGENTS.md`,
  root `CHANGELOG.md`, per-package `CHANGELOG.md`, `.github/ISSUE_TEMPLATE/`,
  `.github/dependabot.yml`.

### Changed

- `GOVERNANCE.md §6` simplified — structural rules (no provider-SDK
  copy-paste, no recorded cassettes, nominative trademark use) replace
  the provider-by-provider ToS-audit ceremony.
- **Ecosystem context updated across `README.md` and `docs/index.md`.**
  "Relation to the parsimony kernel" is replaced by "Relation to the
  parsimony ecosystem" — a three-repo table covering `parsimony`,
  `parsimony-connectors`, and `parsimony-agents` (now Apache-2.0,
  published independently on PyPI), plus a note on the fourth component
  `terminal` (AGPL-3.0 with commercial self-host licensing). This
  repo's own license and role are unchanged; the section expansion
  reflects the full OSS ecosystem becoming publicly visible at once.
- **`GOVERNANCE.md` revision-trigger footnote** no longer references
  `DESIGN-distribution-model.md §11` (an internal planning file not
  present in this repo). The trigger is now stated in terms of
  observable events: kernel-contract or connector-model changes.
- **`packages/sec_edgar/README.md`** corrects the PyPI distribution name
  from `parsimony-sec_edgar` to `parsimony-sec-edgar` (hyphen, not
  underscore, matching the published package name).
- **`packages/sdmx/README.md`** replaces a hardcoded machine-specific
  path (`/home/espinet/ockham/parsimony-connectors`) in the catalog-
  publishing instructions with the generic placeholder
  `<path-to-parsimony-connectors>`.

## [0.6.0]

### Removed

- **`parsimony-financial-reports`**: dropped SDK wrapper with broken upstream client.

### Changed

- **Unified on `parsimony-core>=0.6,<0.7`** across all connector packages.
- **`sec_edgar`**: rewritten as direct SEC HTTP (no `edgartools`); three focused connectors.
- **`polymarket`**: typed Gamma/CLOB connectors replace generic HTTP passthroughs.
- **Catalog search imports**: `parsimony.utils.catalog_search` → `parsimony.catalog.search`.
- **Eval tooling** moved under top-level `tooling/` (not shipped in connector sdists).
- **CI**: `pip-audit` failures are no longer swallowed.
