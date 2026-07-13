<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-connectors-brand-dark.png" />
  <img src="docs/assets/parsimony-connectors-brand-light.png" alt="parsimony-connectors" width="640" />
</picture>

**Ready-made connectors for financial and economic data, built on [parsimony](https://github.com/ockham-sh/parsimony).**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://github.com/ockham-sh/parsimony-connectors/blob/main/pyproject.toml)
[![CI](https://github.com/ockham-sh/parsimony-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/ockham-sh/parsimony-connectors/actions)

</div>

---

This repository holds connectors for 22 sources of financial and economic data: central banks, statistical offices, and market data vendors. Each source is its own pip package. Install the ones you need and parsimony picks them up automatically.

## Quickstart

```bash
pip install parsimony-fred
```

```python
from parsimony_fred import CONNECTORS

fred = CONNECTORS.bind(api_key="...")
print(fred.describe())

result = fred["fred_fetch"](series_id="UNRATE")
print(result.frame.tail())   # tabular results expose .frame
```

Instead of `bind`, you can set the `FRED_API_KEY` environment variable. More in [credentials](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/credentials.md).

## Search comes first

Getting data is two steps: search for the series you want, then fetch it by the ID the search returned. Every package ships both kinds of connectors. Where the provider has a search API, the search connector calls it directly. Where the provider can only list its contents, we build a search catalog with [parsimony's catalog tooling](https://docs.parsimony.dev/catalog/) and publish it on [Hugging Face](https://huggingface.co/parsimony-dev); the catalog downloads and caches locally the first time you search.

## Available connectors

<!-- roster:start -->
|  | Package | Source | Connectors |
|---|---|---|---|
| <a href="https://pypi.org/project/parsimony-alpha-vantage/"><img src="https://www.google.com/s2/favicons?domain=alphavantage.co&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-alpha-vantage`](https://pypi.org/project/parsimony-alpha-vantage/) | [Alpha Vantage](https://www.alphavantage.co) | 29 |
| <a href="https://pypi.org/project/parsimony-bde/"><img src="https://www.google.com/s2/favicons?domain=bde.es&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bde`](https://pypi.org/project/parsimony-bde/) | [Banco de España](https://www.bde.es) | 3 |
| <a href="https://pypi.org/project/parsimony-bdf/"><img src="https://www.google.com/s2/favicons?domain=banque-france.fr&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bdf`](https://pypi.org/project/parsimony-bdf/) | [Banque de France](https://www.banque-france.fr) | 3 |
| <a href="https://pypi.org/project/parsimony-bdp/"><img src="https://www.google.com/s2/favicons?domain=bportugal.pt&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bdp`](https://pypi.org/project/parsimony-bdp/) | [Banco de Portugal](https://www.bportugal.pt) | 3 |
| <a href="https://pypi.org/project/parsimony-bls/"><img src="https://www.google.com/s2/favicons?domain=bls.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bls`](https://pypi.org/project/parsimony-bls/) | [U.S. Bureau of Labor Statistics](https://www.bls.gov) | 5 |
| <a href="https://pypi.org/project/parsimony-boc/"><img src="https://www.google.com/s2/favicons?domain=bankofcanada.ca&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-boc`](https://pypi.org/project/parsimony-boc/) | [Bank of Canada](https://www.bankofcanada.ca) | 3 |
| <a href="https://pypi.org/project/parsimony-boj/"><img src="https://www.google.com/s2/favicons?domain=boj.or.jp&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-boj`](https://pypi.org/project/parsimony-boj/) | [Bank of Japan](https://www.boj.or.jp) | 4 |
| <a href="https://pypi.org/project/parsimony-coingecko/"><img src="https://www.google.com/s2/favicons?domain=coingecko.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-coingecko`](https://pypi.org/project/parsimony-coingecko/) | [CoinGecko](https://www.coingecko.com) | 11 |
| <a href="https://pypi.org/project/parsimony-destatis/"><img src="https://www.google.com/s2/favicons?domain=destatis.de&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-destatis`](https://pypi.org/project/parsimony-destatis/) | [Destatis (German federal statistics)](https://www.destatis.de) | 3 |
| <a href="https://pypi.org/project/parsimony-eia/"><img src="https://www.google.com/s2/favicons?domain=eia.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-eia`](https://pypi.org/project/parsimony-eia/) | [U.S. Energy Information Administration](https://www.eia.gov) | 5 |
| <a href="https://pypi.org/project/parsimony-eodhd/"><img src="https://www.google.com/s2/favicons?domain=eodhd.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-eodhd`](https://pypi.org/project/parsimony-eodhd/) | [EODHD](https://eodhd.com) | 17 |
| <a href="https://pypi.org/project/parsimony-finnhub/"><img src="https://www.google.com/s2/favicons?domain=finnhub.io&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-finnhub`](https://pypi.org/project/parsimony-finnhub/) | [Finnhub](https://finnhub.io) | 12 |
| <a href="https://pypi.org/project/parsimony-fmp/"><img src="https://www.google.com/s2/favicons?domain=financialmodelingprep.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-fmp`](https://pypi.org/project/parsimony-fmp/) | [Financial Modeling Prep](https://financialmodelingprep.com) | 19 |
| <a href="https://pypi.org/project/parsimony-fred/"><img src="https://www.google.com/s2/favicons?domain=fred.stlouisfed.org&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-fred`](https://pypi.org/project/parsimony-fred/) | [FRED (Federal Reserve Economic Data)](https://fred.stlouisfed.org) | 2 |
| <a href="https://pypi.org/project/parsimony-polymarket/"><img src="https://www.google.com/s2/favicons?domain=polymarket.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-polymarket`](https://pypi.org/project/parsimony-polymarket/) | [Polymarket](https://polymarket.com) | 4 |
| <a href="https://pypi.org/project/parsimony-rba/"><img src="https://www.google.com/s2/favicons?domain=rba.gov.au&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-rba`](https://pypi.org/project/parsimony-rba/) | [Reserve Bank of Australia](https://www.rba.gov.au) | 3 |
| <a href="https://pypi.org/project/parsimony-riksbank/"><img src="https://www.google.com/s2/favicons?domain=riksbank.se&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-riksbank`](https://pypi.org/project/parsimony-riksbank/) | [Sveriges Riksbank for parsimony — all five public APIs (SWEA, SWESTR, Monetary Policy, Turnover, Holdings)](https://www.riksbank.se) | 7 |
| <a href="https://pypi.org/project/parsimony-sdmx/"><img src="https://www.google.com/s2/favicons?domain=sdmx.org&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-sdmx`](https://pypi.org/project/parsimony-sdmx/) | [SDMX protocol (ECB, Eurostat, IMF, World Bank)](https://sdmx.org) | 4 |
| <a href="https://pypi.org/project/parsimony-sec-edgar/"><img src="https://www.google.com/s2/favicons?domain=sec.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-sec-edgar`](https://pypi.org/project/parsimony-sec-edgar/) | [SEC EDGAR](https://www.sec.gov) | 12 |
| <a href="https://pypi.org/project/parsimony-snb/"><img src="https://www.google.com/s2/favicons?domain=snb.ch&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-snb`](https://pypi.org/project/parsimony-snb/) | [Swiss National Bank (SNB)](https://www.snb.ch) | 3 |
| <a href="https://pypi.org/project/parsimony-tiingo/"><img src="https://www.google.com/s2/favicons?domain=tiingo.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-tiingo`](https://pypi.org/project/parsimony-tiingo/) | [Tiingo](https://www.tiingo.com) | 13 |
| <a href="https://pypi.org/project/parsimony-treasury/"><img src="https://www.google.com/s2/favicons?domain=fiscaldata.treasury.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-treasury`](https://pypi.org/project/parsimony-treasury/) | [U.S. Treasury (Fiscal Data + Office of Debt Management rate feeds)](https://fiscaldata.treasury.gov) | 4 |
<!-- roster:end -->

The table is generated from the package metadata. Run `make readme-roster` to refresh it.

## Adding a connector

A connector is a small package under `packages/`. Start with the [authoring guide](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/contributing/authoring-a-connector.md) and [CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md). [GOVERNANCE.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/GOVERNANCE.md) covers what we accept and how packages are maintained.

## Documentation

- [Getting started](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/getting-started.md)
- [Connectors](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/connectors.md)
- [Discovery and catalogs](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/discovery-and-catalogs.md)
- [Credentials](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/credentials.md)
- [CLI reference](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/reference/cli.md)

The complete documentation is published at [docs.parsimony.dev](https://docs.parsimony.dev).

## Related projects

- [parsimony](https://github.com/ockham-sh/parsimony) is the framework these connectors are built on.
- [parsimony-agents](https://github.com/ockham-sh/parsimony-agents) is a ready-made data analysis agent built on these connectors.

## Development

```bash
make sync                 # install everything with uv
make verify PKG=fred      # lint + types + tests for one package
make verify-all           # the same for every package
```

`make verify` runs the same checks as CI. See [CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md).

## License

Apache-2.0. See [LICENSE](https://github.com/ockham-sh/parsimony-connectors/blob/main/LICENSE).
