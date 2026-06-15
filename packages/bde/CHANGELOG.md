# Changelog — parsimony-bde

All notable changes to `parsimony-bde` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [0.8.0] — 2026-06-08

A full-process completeness pass over the connector (documentation re-compiled
and every claim re-verified against the live BIEST endpoints).

### Added

- **Bank Lending Survey recovered.** The `pb` catalog chapter lists family/table
  aliases (`PB_1_1.1`) that the BIEST web service rejects with HTTP 412
  ("no existe"), so all 262 of those series were previously un-fetchable. The
  real fetchable codes (`DPB…`) live only inside the bulk `pb.zip`; the
  enumerator now reads them from there, recovering ~350 Bank Lending Survey
  series as searchable and fetchable (`enumerate.py`, `_catalog.parse_pb_zip`).
- **Bilingual catalog.** The published catalog CSVs are Spanish-only, so search
  used to be Spanish-only (the discovery index is lexical/BM25 at this
  cardinality — no multilingual embedding bridge). `build_bde_catalog` now
  enriches each entry's `title` with the English short description from
  `favoritas(idioma=en)` where BdE serves one (Spanish title kept as fallback),
  while the Spanish long description rides along in `description` — both are
  indexed, so an agent searching in English or Spanish gets lexical hits
  (`enrich.py`). The enrichment is batched, retried, and split-on-failure so a
  flaky network can't silently drop a batch's English titles.

### Fixed

- **Catalog de-duplication.** A series can be listed under more than one thematic
  chapter (~24% of raw rows were cross-chapter repeats). `enumerate_bde` now
  de-dups by series code (first chapter in order wins), so the catalog is the
  ~15.5k unique series rather than ~20.5k rows with non-deterministic per-row
  `category`.
- **Frequency-dependent `time_range`.** `bde_fetch` rejected valid daily-series
  ranges (`3M`/`12M`/`36M`) and accepted `MAX` that BdE 412s for daily series.
  The accepted set is now the documented union and BdE validates the
  frequency-specific rule server-side.
- **Typed error for invalid requests.** An unknown series code or a
  frequency-incompatible range (BdE HTTP 412) now surfaces as
  `InvalidParameterError` carrying BdE's own message, instead of a generic
  `ProviderError(412)` that reads like a transient server fault.

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
