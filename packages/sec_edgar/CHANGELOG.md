# Changelog — parsimony-sec-edgar

All notable changes to `parsimony-sec-edgar` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-09

Re-run through the connector guidebook process: all of EDGAR's API surfaces
compiled from the SEC docs and live-probed, the verb set expanded from 4 to 7 so
both completeness questions are answered, two silent-data-loss bugs fixed, and
the monolithic module split to the exemplar layout.

### Added

- **`sec_edgar_full_text_search`** — the native EDGAR full-text search
  (`efts.sec.gov`) over the *content* of every filing since 2001, across all
  ~800k+ filers. This is the authoritative discovery surface (the ticker map
  alone covers only the ~10.4k exchange-listed issuers and cannot search
  content). Filter by `forms`, `start_date`/`end_date`, `ciks`; page with
  `offset`. A returned hit carries the `cik` + `accession` + `document` needed
  to chain straight into `sec_edgar_fetch_filing`. Results are **relevance-ranked**
  (EDGAR's full-text API has no date sort — `sort=date` 500s and other sort params
  are silently ignored; verified live), so "most recent" is a client-side sort on
  `filing_date` over a date-bounded query — called out in the verb's docstring.
- **`sec_edgar_company_concept`** — one XBRL concept's full reported history for
  one company, as a tidy long timeseries (period, value, unit, fiscal
  year/period, form, filed, accession). EDGAR's closest thing to a timeseries.
- **`sec_edgar_frames`** — one XBRL concept for one calendrical period across
  every reporting company (a cross-sectional snapshot, e.g. all filers'
  `AccountsPayableCurrent` for `CY2019Q1I`).

### Fixed

- **`sec_edgar_submissions` dropped filings older than the recent window.** The
  EDGAR submissions JSON keeps only the most-recent ≥1000 filings inline
  (`filings.recent`); older ones page into `filings.files[]`. The old reader saw
  only `recent`, so a prolific filer's history was unreachable (JPMorgan has
  ~158k filings across 67 pages). Added `include_older` (walks the pages) and a
  `form` filter (so an agent can list, e.g., every 10-K back to the 1990s).
- **`sec_edgar_fetch_filing` could not resolve old filings and trusted a viewer
  path.** It resolved the primary document by walking `filings.recent` only (so
  it failed for any accession outside the recent window) and used the
  `primaryDocument` field verbatim, which can point to an XSL *viewer* subpath
  (`xslF345X06/form4.xml`). It now resolves the primary document from the
  accession folder's `index.json` — which works for **any** filing, however old,
  and returns the raw document.

### Changed

- Module restructured from a single ~307-line `__init__.py` to the exemplar
  layout (`_http`, `outputs`, `connectors/{search,filings,xbrl}`, thin facade).
- Full-text-search date gotcha encoded: `startdt`/`enddt` are sent only with
  `dateRange=custom` (the API returns HTTP 500 otherwise — verified live).
- Bump `parsimony-core` pin to `>=0.7,<0.8`.

### Verified

- All 7 verbs live-verified against the real API (throwaway User-Agent at low
  volume): full-text search (`q="climate risk"`, form-filtered), Apple lookup →
  CIK 0000320193, submissions schema + the `include_older` history reach
  (10-Ks back past 2010), `index.json` document resolution (HTML 10-K body),
  Apple `Assets` concept history (values > $1e10), Apple company-facts, and a
  `CY2019Q1I` frame spanning >1000 filers. Stays in
  `EXCLUDED_COMMERCIAL_PROVIDERS` (native search ⇒ no catalog).

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
