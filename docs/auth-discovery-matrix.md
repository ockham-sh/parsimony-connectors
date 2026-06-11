# Auth and discovery matrix

Per-connector reference for credentials and how users discover series before fetch.

| Package | Auth | Env var | Discovery |
|---------|------|---------|-----------|
| parsimony-alpha-vantage | required key | `ALPHA_VANTAGE_API_KEY` | native search |
| parsimony-bde | keyless | — | catalog search (`bde_search`) |
| parsimony-bdf | required key | `BDF_API_KEY` | catalog search |
| parsimony-bdp | keyless | — | catalog search |
| parsimony-bls | optional key (quota) | `BLS_API_KEY` | enumerate only (`enumerate_bls`) |
| parsimony-boc | keyless | — | catalog search |
| parsimony-boj | keyless | — | catalog search (`boj_databases_search`, `boj_series_search`) |
| parsimony-coingecko | required key | `COINGECKO_API_KEY` | native search |
| parsimony-destatis | keyless | — | catalog search |
| parsimony-eia | required key | `EIA_API_KEY` | enumerate only |
| parsimony-eodhd | required key | `EODHD_API_KEY` | native search |
| parsimony-finnhub | required key | `FINNHUB_API_KEY` | native search |
| parsimony-fmp | required key | `FMP_API_KEY` | native search |
| parsimony-fred | required key | `FRED_API_KEY` | native search |
| parsimony-polymarket | keyless | — | enumerate only |
| parsimony-rba | keyless | — | catalog search |
| parsimony-riksbank | optional key (quota) | `RIKSBANK_API_KEY` | catalog search |
| parsimony-sdmx | keyless | — | catalog search (`sdmx_datasets_search`, `sdmx_series_search`) |
| parsimony-sec-edgar | User-Agent required | `SEC_EDGAR_USER_AGENT` | tool connectors only |
| parsimony-snb | keyless | — | catalog search |
| parsimony-tiingo | required key | `TIINGO_API_KEY` | native search |
| parsimony-treasury | keyless | — | catalog search |

Catalog-backed packages depend on `parsimony-core[catalog]` and load hosted snapshots from
`hf://parsimony-dev/*` by default, with lazy local rebuild when a bundle is missing.

**Dict-returning connectors** (no `.df` on the result): `polymarket_market_prices`,
`sec_edgar_company_facts`, `sec_edgar_fetch_filing`, `coingecko_coin_detail`,
`eodhd_fundamentals`.
