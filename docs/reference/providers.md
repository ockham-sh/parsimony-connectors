# Provider reference

The 22 officially-maintained connector packages, one per data provider. Each ships as its own
PyPI distribution named `parsimony-<name>` and registers through the `parsimony.providers`
entry-point, so installing it is all the wiring there is:

```bash
pip install parsimony-core parsimony-fred parsimony-sdmx
```

```python
from parsimony import discover

connectors = discover.load_all()          # every installed parsimony-* package
connectors = discover.load("fred", "sdmx") # or just the ones you name
```

Every package exposes the same shape: a `CONNECTORS` collection of plain synchronous
connectors (never async), addressed by name (`connectors["fred_fetch"]`). A connector is
either a **discovery** verb (search/enumerate — "what data exists?") or a **fetch** verb
("give me the values for this code"). The agent loop is always *search to find a code, then
fetch that code*.

## Two discovery models

Discovery comes in two shapes. The difference is only *where the search runs*; the fetch side
is identical.

- **Native search** — the provider ships a usable search/screener endpoint, so the
  `<provider>_search` connector wraps it. The query goes live to the provider; no catalog is
  built. Nine providers: `alpha_vantage`, `coingecko`, `eodhd`, `finnhub`, `fmp`, `fred`,
  `polymarket`, `sec_edgar`, `tiingo`.
- **Catalog-backed** — the provider has no usable native search (it only fetches by exact
  code, or its search is too narrow to enumerate the universe), so the maintainers build and
  publish a searchable catalog at `hf://parsimony-dev/<provider>`. The `<provider>_search`
  connector queries that local snapshot — fast, offline-capable, and consuming no provider
  quota. Thirteen providers: `bde`, `bdf`, `bdp`, `bls`, `boc`, `boj`, `destatis`, `eia`,
  `rba`, `riksbank`, `sdmx`, `snb`, `treasury`.

Catalog-backed packages depend on `parsimony-core[catalog]`; native-search packages depend on
plain `parsimony-core`. For the search/index model in depth, see
[../concepts/discovery-and-catalogs.md](../concepts/discovery-and-catalogs.md). For how keys
are supplied and protected, see [../concepts/credentials.md](../concepts/credentials.md).

## Auth shapes

Every provider falls into one of four auth shapes — the four combinations of two independent
declarations, `secrets=` (parameters to hide) and `requires=` (env vars a call needs). The
`Env var` column below reflects `requires=`; a provider is keyless exactly when `requires=` is
empty. See [../concepts/credentials.md](../concepts/credentials.md) for the framing.

- **Required key** — declares `secrets=` and `requires=`; fast-fails before any network call if
  no key resolves, raising `UnauthorizedError` that names the env var. Set `<PROVIDER>_API_KEY`
  or bind via `load()`.
- **Optional key (quota)** — declares `secrets=` but `requires=()`: works keyless at a lower
  rate limit, a key only raises the quota (and for `bls`, enriches the output). Never fast-fails.
- **Keyless** — no `secrets=`, no `requires=`; call it directly.
- **UA-required** — needs no secret, but `sec_edgar` requires a `User-Agent` header
  (`SEC_EDGAR_USER_AGENT`, name + email) under SEC's fair-access policy. Declared as
  `requires=` with no `secrets=` — an env-resolved header, not a redacted secret; a missing
  value fast-fails.

## Provider table

The **Discovery** column marks native-search (live) versus catalog-backed (`hf://`). The
**Key connectors** column lists the user-facing discovery and fetch verbs (the `CONNECTORS`
collection also includes the `enumerate_*` builders that operators run when building a
catalog; those are not part of the everyday search→fetch loop).

**Auth applies per connector, not per package** — read the two columns together. On a
catalog-backed provider the `<provider>_search` verb reads the published snapshot, so it
works with **no key even when the package's Auth says "required key"**; only the fetch verbs
call the keyed API. `parsimony-eia` is the clearest case: `eia_search` needs nothing,
`eia_fetch` needs `EIA_API_KEY`. On a native-search provider there is no snapshot — the
search itself is the keyed call, so `eodhd_search` / `finnhub_search` / `tiingo_search` /
`coingecko_search` all fast-fail without a key. Each search connector states which it is in
its own `describe()`.

| Package (PyPI) | Source | Auth | Env var | Discovery | Key connectors |
| --- | --- | --- | --- | --- | --- |
| [`parsimony-alpha-vantage`](https://pypi.org/project/parsimony-alpha-vantage/) | [Alpha Vantage](https://www.alphavantage.co) | required key | `ALPHA_VANTAGE_API_KEY` | native | `alpha_vantage_search`, `alpha_vantage_quote`, `alpha_vantage_daily`, `alpha_vantage_income_statement`, … (29 total) |
| [`parsimony-bde`](https://pypi.org/project/parsimony-bde/) | [Banco de España](https://www.bde.es) | keyless | — | catalog | `bde_search`, `bde_fetch` |
| [`parsimony-bdf`](https://pypi.org/project/parsimony-bdf/) | [Banque de France](https://www.banque-france.fr) | required key | `BDF_API_KEY` | catalog | `bdf_search`, `bdf_fetch` |
| [`parsimony-bdp`](https://pypi.org/project/parsimony-bdp/) | [Banco de Portugal](https://www.bportugal.pt) | keyless | — | catalog | `bdp_search`, `bdp_fetch` |
| [`parsimony-bls`](https://pypi.org/project/parsimony-bls/) | [U.S. Bureau of Labor Statistics](https://www.bls.gov) | optional key (quota) | — | catalog (two-tier) | `bls_surveys_search`, `bls_series_search`, `bls_fetch` |
| [`parsimony-boc`](https://pypi.org/project/parsimony-boc/) | [Bank of Canada](https://www.bankofcanada.ca) | keyless | — | catalog | `boc_search`, `boc_fetch` |
| [`parsimony-boj`](https://pypi.org/project/parsimony-boj/) | [Bank of Japan](https://www.boj.or.jp) | keyless | — | catalog (two-tier) | `boj_databases_search`, `boj_series_search`, `boj_fetch` |
| [`parsimony-coingecko`](https://pypi.org/project/parsimony-coingecko/) | [CoinGecko](https://www.coingecko.com) | required key | `COINGECKO_API_KEY` | native | `coingecko_search`, `coingecko_price`, `coingecko_markets`, `coingecko_coin_detail`, … (11 total) |
| [`parsimony-destatis`](https://pypi.org/project/parsimony-destatis/) | [Destatis (German federal statistics)](https://www.destatis.de) | keyless | — | catalog | `destatis_search`, `destatis_fetch` |
| [`parsimony-eia`](https://pypi.org/project/parsimony-eia/) | [U.S. Energy Information Administration](https://www.eia.gov) | required key | `EIA_API_KEY` | catalog | `eia_search`, `eia_fetch`, `eia_fetch_series`, `eia_facets` |
| [`parsimony-eodhd`](https://pypi.org/project/parsimony-eodhd/) | [EODHD](https://eodhd.com) | required key | `EODHD_API_KEY` | native | `eodhd_search`, `eodhd_eod`, `eodhd_fundamentals`, … (17 total) |
| [`parsimony-finnhub`](https://pypi.org/project/parsimony-finnhub/) | [Finnhub](https://finnhub.io) | required key | `FINNHUB_API_KEY` | native | `finnhub_search`, `finnhub_quote`, `finnhub_profile`, `finnhub_basic_financials`, … (12 total) |
| [`parsimony-fmp`](https://pypi.org/project/parsimony-fmp/) | [Financial Modeling Prep](https://financialmodelingprep.com) | required key | `FMP_API_KEY` | native | `fmp_search`, `fmp_quotes`, `fmp_prices`, `fmp_screener`, … (19 total) |
| [`parsimony-fred`](https://pypi.org/project/parsimony-fred/) | [FRED (Federal Reserve Economic Data)](https://fred.stlouisfed.org) | required key | `FRED_API_KEY` | native | `fred_search`, `fred_fetch` |
| [`parsimony-polymarket`](https://pypi.org/project/parsimony-polymarket/) | [Polymarket](https://polymarket.com) | keyless | — | native | `polymarket_markets`, `polymarket_events`, `polymarket_market_prices` |
| [`parsimony-rba`](https://pypi.org/project/parsimony-rba/) | [Reserve Bank of Australia](https://www.rba.gov.au) | keyless | — | catalog | `rba_search`, `rba_fetch` |
| [`parsimony-riksbank`](https://pypi.org/project/parsimony-riksbank/) | [Swedish Riksbank](https://www.riksbank.se) | optional key (quota) | — | catalog | `riksbank_search`, `riksbank_fetch`, `riksbank_swestr_fetch`, `riksbank_monetary_policy_fetch`, `riksbank_turnover_fetch`, `riksbank_holdings_fetch` |
| [`parsimony-sdmx`](https://pypi.org/project/parsimony-sdmx/) | [SDMX protocol](https://sdmx.org) | keyless | — | catalog | `sdmx_datasets_search`, `sdmx_series_search`, `sdmx_dimension_search`, `sdmx_fetch` |
| [`parsimony-sec-edgar`](https://pypi.org/project/parsimony-sec-edgar/) | [SEC EDGAR](https://www.sec.gov) | UA-required | `SEC_EDGAR_USER_AGENT` | native | `sec_edgar_full_text_search`, `sec_edgar_find_company`, `sec_edgar_submissions`, `sec_edgar_fetch_filing`, `sec_edgar_company_facts`, … (12 total) |
| [`parsimony-snb`](https://pypi.org/project/parsimony-snb/) | [Swiss National Bank](https://www.snb.ch) | keyless | — | catalog | `snb_search`, `snb_fetch` |
| [`parsimony-tiingo`](https://pypi.org/project/parsimony-tiingo/) | [Tiingo](https://www.tiingo.com) | required key | `TIINGO_API_KEY` | native | `tiingo_search`, `tiingo_eod`, `tiingo_meta`, `tiingo_fundamentals_meta`, … (13 total) |
| [`parsimony-treasury`](https://pypi.org/project/parsimony-treasury/) | [U.S. Treasury](https://fiscaldata.treasury.gov) | keyless | — | catalog | `treasury_search`, `treasury_fetch`, `treasury_rates_fetch` |

The authoritative connector list for any package is its `CONNECTORS` collection — list it at
runtime with `discover.load("<name>").names()`, or across everything with `parsimony list`
(see [cli.md](cli.md)).

## Dict-returning connectors

Most connectors return a `Result` whose tabular payload is `result.frame`. A few return a
nested record instead — for these, the payload is `result.raw` (a dict or list of dicts) and
`result.frame` raises `TypeError` (the result is not tabular):

- `coingecko_coin_detail`
- `eodhd_fundamentals`
- `finnhub_profile`, `finnhub_basic_financials`
- `polymarket_market_prices`
- `sec_edgar_company_facts`, `sec_edgar_fetch_filing`
- `tiingo_meta`, `tiingo_fundamentals_meta`

## Per-provider notes

- **`sdmx`** wires four of the agencies SDMX advertises: **ECB**, **Eurostat** (`ESTAT`),
  **IMF** (`IMF_DATA`), and **World Bank** (`WB_WDI`). `sdmx_fetch` and the search verbs reject
  any agency outside this set. Discovery is split: `sdmx_datasets_search` finds dataflows,
  `sdmx_series_search` finds individual series, and `sdmx_dimension_search` resolves a
  dimension's valid codes — all over the published catalog. `sdmx_fetch` then pulls a series
  (or a batch) live. Only published flows are searchable; an unpublished flow hard-errors.
- **`riksbank`** is **one catalog over five products** — SWEA (rates/FX), SWESTR (the Swedish
  Krona Short-Term Rate), Monetary Policy Data, Turnover Statistics, and Holdings. A single
  `riksbank_search` returns hits whose `code` shape routes the agent to the matching fetch
  verb (`riksbank_fetch`, `riksbank_swestr_fetch`, `riksbank_monetary_policy_fetch`,
  `riksbank_turnover_fetch`, `riksbank_holdings_fetch`). The key is optional and lifts the
  keyless quota only.
- **`treasury`** has **two fetch verbs** behind one `treasury_search`. The search hit carries
  a `source` column that selects the verb: `source=fiscal_data` →
  `treasury_fetch(endpoint=...)` (the Fiscal Data JSON API); `source=treasury_rates` →
  `treasury_rates_fetch(feed=...)` (the Office of Debt Management daily rate feeds).
- **`bls`** and **`boj`** are **two-tier** catalogs: first search the survey/database index,
  then search series within the chosen survey/database. For `bls` the key is optional (quota
  plus richer output); `boj` is keyless.
- **`sec_edgar`** needs no API key but every request must carry a `User-Agent` identifying the
  requester. Set `SEC_EDGAR_USER_AGENT` to a `name email` string before calling; a missing
  value fast-fails. Discovery is `sec_edgar_full_text_search` (live EDGAR full-text search)
  plus `sec_edgar_find_company` (ticker/CIK/name lookup).
- **`polymarket`** has no `<provider>_search`; discovery is the two live enumerators
  `polymarket_markets` and `polymarket_events`. `polymarket_market_prices` returns the current
  CLOB price as a dict.

## Catalog overrides

Every catalog-backed connector resolves its snapshot from `hf://parsimony-dev/<provider>` by
default, falling back to a lazy local rebuild. Point any connector at an alternate snapshot
(a local build, a CI fixture, a fork) with the per-provider override env var
`PARSIMONY_<PROVIDER>_CATALOG_URL`, and relocate the on-disk cache root with
`PARSIMONY_CACHE_DIR`. These are not credentials. See [cli.md](cli.md) for the full list of
runtime environment variables and the cache CLI.

## See also

- [../concepts/discovery-and-catalogs.md](../concepts/discovery-and-catalogs.md) — the
  search→fetch model, catalog structure, and indexing policy.
- [../concepts/credentials.md](../concepts/credentials.md) — auth shapes, `secrets=` /
  `.bind`, and how to supply a key.
- [../guides/using-connectors.md](../guides/using-connectors.md) — loading, binding, and the
  search→fetch loop in practice.
- [cli.md](cli.md) — the `parsimony` CLI, Make targets, and runtime environment variables.
