# Changelog — parsimony-fmp

All notable changes to `parsimony-fmp` will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- `fmp_prices`, `fmp_income_statements`, `fmp_balance_sheet_statements` and
  `fmp_cash_flow_statements` return rows **oldest-first**. FMP serves them
  newest-first; every other time-series connector in the library is ascending, so
  generic code reading `.iloc[-1]` as "the latest period" silently picked the oldest
  one here. Same rows, different order. (#74)
- `eps` / `epsDiluted` declare their period convention: the statement's own period,
  never TTM. Alpha Vantage's `OVERVIEW` `EPS` is TTM and will not match. (#78)
- **Plan tags corrected and now predictive.** Every verb's tag was re-derived from
  FMP's plan-comparison matrix and checked against a live Basic key on 2026-07-23;
  11 of 19 were wrong. Verification scope: the Basic column was measured, the
  Starter/Premium/Ultimate boundaries come from FMP's published matrix and inherit
  any staleness in it. The tag vocabulary now matches FMP's actual ladder — Basic
  (free), Starter, Premium, Ultimate — replacing a `[Professional+]` plan FMP does
  not sell. Notable corrections: `fmp_index_constituents` `[All plans]` → `[Premium+]`,
  `fmp_analyst_estimates` `[Professional+]` → `[Basic+]`, `fmp_quotes` `[Starter+]` →
  `[Premium+]`, `fmp_earnings_transcript` / `fmp_institutional_positions` →
  `[Ultimate]`, `fmp_insider_trades` → `[Starter+]`, and `fmp_taxonomy` `[Paid]` →
  `[Starter+]`. `fmp_prices` and `fmp_news` carry compound tags because they are gated
  per argument, not per endpoint.
- Two gates are **not** endpoint-level and are now documented on the verbs they affect:
  on Basic, symbol-scoped endpoints serve only FMP's fixed 87-ticker sample (this is
  the "per-symbol gating with no obvious pattern" from the field report — reproduced
  12/12 on its exact basket), and date-windowed verbs reject an over-long window rather
  than truncating it. Both still raise `PaymentRequiredError`, so keep catching it. (#73)
- The `Demo: 3 symbols (AAPL, TSLA, MSFT)` note on seven verbs was wrong and is gone:
  the free sample is 87 tickers, and paid coverage widens by region (US → US/UK/Canada
  → global), not to "all symbols".

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
