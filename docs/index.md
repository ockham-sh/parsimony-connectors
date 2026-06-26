<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/parsimony-connectors-brand-dark.png" />
  <img src="docs/assets/parsimony-connectors-brand-light.png" alt="parsimony-connectors" width="640" />
</picture>

**Officially-maintained data connectors for the [parsimony](https://github.com/ockham-sh/parsimony) framework — one connector contract across every source, each its own pip install.**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://github.com/ockham-sh/parsimony-connectors/blob/main/pyproject.toml)
[![CI](https://github.com/ockham-sh/parsimony-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/ockham-sh/parsimony-connectors/actions)

</div>

---

## What it is

parsimony-connectors is the official connector library for [parsimony](https://github.com/ockham-sh/parsimony), the kernel that turns a plain Python function into a typed, provenance-tracked data connector. This monorepo packages **22 production data sources** — central banks, national statistical agencies, and market-data vendors — as individual `parsimony-<name>` distributions, plus the shared `parsimony-shared` helper library (23 distributions in total). You install only the providers you need; each registers itself at runtime through the `parsimony.providers` entry point, so there is no central registry to wire up.

Every connector gives you two things:

1. **A fetch surface.** A synchronous `def` you call to pull data (`result = fred_fetch(series_id="UNRATE")`). The framework wraps the raw `DataFrame` your function returns into a typed `Result` with automatic provenance, and maps upstream failures to a small, agent-facing error taxonomy.
2. **A discovery surface.** A `<provider>_search` to find the code you want before fetching — either the provider's own search API (*native-search* providers) or a prebuilt hybrid-search catalog hosted on Hugging Face (*catalog-backed* providers).

## Install

```bash
pip install parsimony-core parsimony-fred parsimony-sdmx   # the kernel + the connectors you want
```

Each connector is a separate distribution, so you pull only what you use. Catalog-backed connectors (e.g. `parsimony-sdmx`, `parsimony-treasury`) automatically bring the kernel's `[catalog]` extra for hybrid search. Requires Python 3.11+.

## Quickstart

```python
from parsimony import discover

connectors = discover.load_all()                         # every installed parsimony-* provider
unrate = connectors["fred_fetch"](series_id="UNRATE")    # returns a typed Result
print(unrate.df.tail())                                  # tabular results expose .df
```

Use `discover.load("fred", "sdmx")` to load a named subset. The kernel finds installed providers through Python entry points (`parsimony.providers`): add a `parsimony-*` package and it appears in `connectors`; remove it and it disappears. There is no central registry.

## Connector roster

Every connector ships as its own PyPI distribution, so you install only the providers you need. The "Connectors" column counts the search, fetch, and enumerate functions each package exposes.

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
| <a href="https://pypi.org/project/parsimony-polymarket/"><img src="https://www.google.com/s2/favicons?domain=polymarket.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-polymarket`](https://pypi.org/project/parsimony-polymarket/) | [Polymarket](https://polymarket.com) | 3 |
| <a href="https://pypi.org/project/parsimony-rba/"><img src="https://www.google.com/s2/favicons?domain=rba.gov.au&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-rba`](https://pypi.org/project/parsimony-rba/) | [Reserve Bank of Australia](https://www.rba.gov.au) | 3 |
| <a href="https://pypi.org/project/parsimony-riksbank/"><img src="https://www.google.com/s2/favicons?domain=riksbank.se&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-riksbank`](https://pypi.org/project/parsimony-riksbank/) | [Sveriges Riksbank for parsimony — all five public APIs (SWEA, SWESTR, Monetary Policy, Turnover, Holdings)](https://www.riksbank.se) | 7 |
| <a href="https://pypi.org/project/parsimony-sdmx/"><img src="https://www.google.com/s2/favicons?domain=sdmx.org&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-sdmx`](https://pypi.org/project/parsimony-sdmx/) | [SDMX protocol (ECB, Eurostat, IMF, World Bank)](https://sdmx.org) | 6 |
| <a href="https://pypi.org/project/parsimony-sec-edgar/"><img src="https://www.google.com/s2/favicons?domain=sec.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-sec-edgar`](https://pypi.org/project/parsimony-sec-edgar/) | [SEC EDGAR](https://www.sec.gov) | 12 |
| <a href="https://pypi.org/project/parsimony-snb/"><img src="https://www.google.com/s2/favicons?domain=snb.ch&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-snb`](https://pypi.org/project/parsimony-snb/) | [Swiss National Bank (SNB)](https://www.snb.ch) | 3 |
| <a href="https://pypi.org/project/parsimony-tiingo/"><img src="https://www.google.com/s2/favicons?domain=tiingo.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-tiingo`](https://pypi.org/project/parsimony-tiingo/) | [Tiingo](https://www.tiingo.com) | 13 |
| <a href="https://pypi.org/project/parsimony-treasury/"><img src="https://www.google.com/s2/favicons?domain=fiscaldata.treasury.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-treasury`](https://pypi.org/project/parsimony-treasury/) | [U.S. Treasury (Fiscal Data + Office of Debt Management rate feeds)](https://fiscaldata.treasury.gov) | 4 |
<!-- roster:end -->

The roster is generated from `packages/*/pyproject.toml` and the connector source tree. Run `make readme-roster` to refresh it.

## Discovery models

Connectors come in two flavours, depending on how you find the series you want.

**Native-search providers** wrap the provider's own search endpoint. A `<provider>_search` helper queries the upstream API directly and returns matches you can feed straight into the fetch function.

**Catalog-backed providers** ship a `<provider>_search` over a prebuilt catalog hosted on Hugging Face at `hf://parsimony-dev/<provider>`. The catalog loads lazily on first use, caches locally, and rebuilds from source if it is missing. This makes large, slow-to-enumerate universes searchable without hammering the upstream provider.

Either way the fetch surface is the same: a plain `def` that returns a `Result`.

## Engineering guarantees

**Conformance is the merge gate.** Every connector must pass `parsimony.testing.assert_plugin_valid()` to merge or release, run in CI via `parsimony list --strict`. The suite checks `CONNECTORS` exports and non-empty descriptions. Malformed plugins never reach PyPI.

**Secret-leak tests are baked in.** Every connector inherits `ErrorMappingSuite` from `test_support`, which injects a `CANARY_KEY` and asserts it never appears in a `ConnectorError` message, in `Result.provenance`, or in the `to_llm()` projection. One template, enforced everywhere.

**Per-package OIDC publishing.** Each connector has its own version, its own `release.yml` trigger, and its own PyPI Trusted Publisher. A bug fix in `parsimony-fred` does not block a feature ship in `parsimony-alpha-vantage`.

**The kernel is editable in CI.** The root `pyproject.toml` pins `parsimony-core` via an editable path (`../parsimony`). Kernel breaking changes get verified against every connector in a single PR before either side releases.

**Provider quirks live in the connector, not the kernel.** The Reserve Bank of Australia fronts its data with Akamai, so `parsimony-rba` uses `curl_cffi` for TLS fingerprint impersonation. SDMX dynamically discovers dataflows and wires 4 of 7 advertised agencies — ECB, Eurostat, IMF, and the World Bank — publishing a Hugging Face catalog per `(agency, dataset)`.

## Repository layout

One repository, many PyPI distributions:

```text
parsimony-connectors/
├── pyproject.toml             # uv workspace root, editable kernel pin
├── test_support/              # shared test fixtures (ErrorMappingSuite, CANARY_KEY)
├── scripts/
│   └── gen_roster.py          # rebuilds the roster table in this README
└── packages/
    ├── fred/                  # → parsimony-fred on PyPI
    │   ├── pyproject.toml
    │   ├── parsimony_fred/
    │   └── tests/
    ├── sdmx/                  # → parsimony-sdmx on PyPI
    │   ├── parsimony_sdmx/
    │   ├── scripts/           # HF catalog publisher
    │   ├── eval/              # retrieval evaluation harness
    │   └── tests/
    └── ...
```

Connectors pin `parsimony-core>=0.7,<0.8`; catalog-backed packages pull the `[catalog]` extra. The pin is a plain version range on the kernel — there is no separate contract-version declaration.

### Local development

```bash
make sync                 # uv sync --all-extras --all-packages
make verify PKG=fred      # ruff + mypy + pytest + strict plugin listing
make verify-all           # the same, across every package
make readme-roster        # regenerate the connector roster table above
```

`make verify` mirrors the CI pipeline exactly. If it passes locally, CI passes too.

## Building catalogs

Catalog-backed connectors include a provider-owned `packages/<provider>/scripts/build_catalog.py`. Building is an operator workflow — the user-facing package surface stays `CONNECTORS`, while the script enumerates rows, builds indexes, and uploads to Hugging Face. See [docs/guides/building-catalogs.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/guides/building-catalogs.md) for the full runbook.

## Documentation

- **Getting started:** [docs/getting-started.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/getting-started.md)
- **Concepts:** [connectors](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/connectors.md), [discovery and catalogs](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/discovery-and-catalogs.md), [credentials](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/credentials.md), [errors](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/concepts/errors.md)
- **Guides:** [using connectors](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/guides/using-connectors.md), [building catalogs](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/guides/building-catalogs.md)
- **Reference:** [providers](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/reference/providers.md), [CLI](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/reference/cli.md)
- **Contributing:** [authoring a connector](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/contributing/authoring-a-connector.md)

## Relation to the parsimony ecosystem

This repository is one part of the open-source Ockham data stack:

| Repo | PyPI distribution | License | Role |
|---|---|---|---|
| [`parsimony`](https://github.com/ockham-sh/parsimony) | `parsimony-core` | Apache 2.0 | Kernel: connector primitives, entry-point discovery, conformance suite |
| **`parsimony-connectors`** (this repo) | `parsimony-<name>` (22 connectors + `parsimony-shared`) | Apache 2.0 | Officially-maintained provider connectors |
| [`parsimony-agents`](https://github.com/ockham-sh/parsimony-agents) | `parsimony-agents` | Apache 2.0 | Agent loop and orchestration built on the connector layer |
| [ockham (`terminal`)](https://github.com/ockham-sh/terminal) | — | AGPLv3 | Self-hosted deployment product (coming soon) |

**Kernel.** `parsimony-core` is a thin shell: connector primitives, entry-point discovery, conformance suite, scaffolding. It knows nothing about specific providers. Connectors depend on the kernel through the stable `parsimony.providers` entry-point contract and a version-range pin, so connector and kernel release cadences are independent.

Adding a new public data source means adding a package under `packages/` and passing the conformance suite.

## Contributing

- First read: [CONTRIBUTING.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/CONTRIBUTING.md). Local dev workflow, conformance gate, how to add a new connector.
- Authoring guide: [docs/contributing/authoring-a-connector.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/docs/contributing/authoring-a-connector.md).
- Governance: [GOVERNANCE.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/GOVERNANCE.md). Acceptance criteria, stewardship, deprecation, graduation.
- Security: [SECURITY.md](https://github.com/ockham-sh/parsimony-connectors/blob/main/SECURITY.md). How to report vulnerabilities.

Anyone may contribute. The conformance suite is the merge gate.

## License

Apache 2.0. Every connector that ships from this repository agrees to Apache 2.0 redistribution. See [GOVERNANCE.md §6](https://github.com/ockham-sh/parsimony-connectors/blob/main/GOVERNANCE.md#6-licence) for how this intersects with third-party provider terms.
