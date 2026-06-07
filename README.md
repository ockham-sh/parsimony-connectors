# parsimony-connectors

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](pyproject.toml)
[![CI](https://github.com/ockham-sh/parsimony-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/ockham-sh/parsimony-connectors/actions)
[![Docs](https://img.shields.io/badge/docs-parsimony.dev-blue)](https://docs.parsimony.dev)

Officially-maintained connectors for the [parsimony](https://github.com/ockham-sh/parsimony) framework. One plain connector contract across every source, separate PyPI distributions so you install only what you need.

## Quickstart

```bash
pip install parsimony-core parsimony-fred parsimony-sdmx
```

```python
import asyncio
from parsimony import discover

async def main():
    connectors = discover.load_all()
    fred = await connectors["fred_fetch"](series_id="UNRATE")
    ecb = await connectors["sdmx_fetch"](
        dataset_key="ECB-ICP",
        series_key="M.U2.N.000000.4.ANR",
    )

asyncio.run(main())
```

The kernel finds every installed `parsimony-*` package through Python entry-points. Add another connector and it shows up in `connectors`; remove it and it disappears. There is no central registry.

## Connector roster

Every connector ships as its own PyPI distribution. The "Tool surface" column shows how many of each connector's functions are tagged `tool` (cheap discovery helpers for agents) versus how many are bulk-fetch (returned into a Python variable in a code interpreter).

<!-- roster:start -->
|  | Package | Source | Connectors | Tool surface |
|---|---|---|---|---|
| <a href="https://pypi.org/project/parsimony-alpha-vantage/"><img src="https://www.google.com/s2/favicons?domain=alphavantage.co&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-alpha-vantage`](https://pypi.org/project/parsimony-alpha-vantage/) | [Alpha Vantage](https://www.alphavantage.co) | 29 | 4 of 29 |
| <a href="https://pypi.org/project/parsimony-bde/"><img src="https://www.google.com/s2/favicons?domain=bde.es&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bde`](https://pypi.org/project/parsimony-bde/) | [Banco de España](https://www.bde.es) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-bdf/"><img src="https://www.google.com/s2/favicons?domain=banque-france.fr&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bdf`](https://pypi.org/project/parsimony-bdf/) | [Banque de France](https://www.banque-france.fr) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-bdp/"><img src="https://www.google.com/s2/favicons?domain=bportugal.pt&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bdp`](https://pypi.org/project/parsimony-bdp/) | [Banco de Portugal](https://www.bportugal.pt) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-bls/"><img src="https://www.google.com/s2/favicons?domain=bls.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-bls`](https://pypi.org/project/parsimony-bls/) | [U.S. Bureau of Labor Statistics](https://www.bls.gov) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-boc/"><img src="https://www.google.com/s2/favicons?domain=bankofcanada.ca&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-boc`](https://pypi.org/project/parsimony-boc/) | [Bank of Canada](https://www.bankofcanada.ca) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-boj/"><img src="https://www.google.com/s2/favicons?domain=boj.or.jp&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-boj`](https://pypi.org/project/parsimony-boj/) | [Bank of Japan](https://www.boj.or.jp) | 3 | 2 of 3 |
| <a href="https://pypi.org/project/parsimony-coingecko/"><img src="https://www.google.com/s2/favicons?domain=coingecko.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-coingecko`](https://pypi.org/project/parsimony-coingecko/) | [CoinGecko](https://www.coingecko.com) | 11 | 3 of 11 |
| <a href="https://pypi.org/project/parsimony-destatis/"><img src="https://www.google.com/s2/favicons?domain=destatis.de&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-destatis`](https://pypi.org/project/parsimony-destatis/) | [Destatis (German federal statistics)](https://www.destatis.de) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-eia/"><img src="https://www.google.com/s2/favicons?domain=eia.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-eia`](https://pypi.org/project/parsimony-eia/) | [U.S. Energy Information Administration](https://www.eia.gov) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-eodhd/"><img src="https://www.google.com/s2/favicons?domain=eodhd.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-eodhd`](https://pypi.org/project/parsimony-eodhd/) | [EODHD](https://eodhd.com) | 17 | 5 of 17 |
| <a href="https://pypi.org/project/parsimony-finnhub/"><img src="https://www.google.com/s2/favicons?domain=finnhub.io&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-finnhub`](https://pypi.org/project/parsimony-finnhub/) | [Finnhub](https://finnhub.io) | 12 | 1 of 12 |
| <a href="https://pypi.org/project/parsimony-fmp/"><img src="https://www.google.com/s2/favicons?domain=financialmodelingprep.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-fmp`](https://pypi.org/project/parsimony-fmp/) | [Financial Modeling Prep](https://financialmodelingprep.com) | 19 | 7 of 19 |
| <a href="https://pypi.org/project/parsimony-fred/"><img src="https://www.google.com/s2/favicons?domain=fred.stlouisfed.org&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-fred`](https://pypi.org/project/parsimony-fred/) | [FRED (Federal Reserve Economic Data)](https://fred.stlouisfed.org) | 2 | 1 of 2 |
| <a href="https://pypi.org/project/parsimony-polymarket/"><img src="https://www.google.com/s2/favicons?domain=polymarket.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-polymarket`](https://pypi.org/project/parsimony-polymarket/) | [Polymarket](https://polymarket.com) | 3 | 3 of 3 |
| <a href="https://pypi.org/project/parsimony-rba/"><img src="https://www.google.com/s2/favicons?domain=rba.gov.au&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-rba`](https://pypi.org/project/parsimony-rba/) | [Reserve Bank of Australia](https://www.rba.gov.au) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-riksbank/"><img src="https://www.google.com/s2/favicons?domain=riksbank.se&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-riksbank`](https://pypi.org/project/parsimony-riksbank/) | [Swedish Riksbank](https://www.riksbank.se) | 2 | 0 of 2 |
| <a href="https://pypi.org/project/parsimony-sdmx/"><img src="https://www.google.com/s2/favicons?domain=sdmx.org&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-sdmx`](https://pypi.org/project/parsimony-sdmx/) | [SDMX protocol (ECB, Eurostat, IMF, OECD, BIS, World Bank, ILO)](https://sdmx.org) | 4 | 2 of 4 |
| <a href="https://pypi.org/project/parsimony-sec-edgar/"><img src="https://www.google.com/s2/favicons?domain=sec.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-sec-edgar`](https://pypi.org/project/parsimony-sec-edgar/) | [SEC Edgar](https://www.sec.gov) | 4 | 4 of 4 |
| <a href="https://pypi.org/project/parsimony-snb/"><img src="https://www.google.com/s2/favicons?domain=snb.ch&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-snb`](https://pypi.org/project/parsimony-snb/) | [Swiss National Bank](https://www.snb.ch) | 1 | 0 of 1 |
| <a href="https://pypi.org/project/parsimony-tiingo/"><img src="https://www.google.com/s2/favicons?domain=tiingo.com&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-tiingo`](https://pypi.org/project/parsimony-tiingo/) | [Tiingo](https://www.tiingo.com) | 13 | 1 of 13 |
| <a href="https://pypi.org/project/parsimony-treasury/"><img src="https://www.google.com/s2/favicons?domain=fiscaldata.treasury.gov&sz=64" width="16" height="16" alt="" /></a> | [`parsimony-treasury`](https://pypi.org/project/parsimony-treasury/) | [U.S. Treasury](https://fiscaldata.treasury.gov) | 2 | 0 of 2 |
<!-- roster:end -->

The roster is generated from `packages/*/pyproject.toml` and the connector source tree. Run `make readme-roster` to refresh it.

## Engineering guarantees

**Conformance is the merge gate.** Every connector must pass `parsimony.testing.assert_plugin_valid()` to merge or release. The suite checks `CONNECTORS` exports and non-empty descriptions. Malformed plugins never reach PyPI.

**Secret-leak tests are baked in.** Every connector inherits `ErrorMappingSuite` from `test_support`, which injects a `CANARY_KEY` and asserts it never appears in a `ConnectorError` message, in `Result.provenance`, or in the `to_llm()` projection. One template, enforced everywhere.

**Per-package OIDC publishing.** Each connector has its own version, its own `release.yml` trigger, and its own PyPI Trusted Publisher. A bug fix in `parsimony-fred` does not block a feature ship in `parsimony-alpha-vantage`.

**The kernel is editable in CI.** The root `pyproject.toml` pins `parsimony-core` via an editable path (`../parsimony`). Kernel breaking changes get verified against every connector in a single PR before either side releases.

**Provider quirks live in the connector, not the kernel.** The Reserve Bank of Australia fronts its data with Akamai, so `parsimony-rba` uses `curl_cffi` for TLS fingerprint impersonation. SDMX dynamically discovers 8000+ dataflows across seven agencies and publishes one Hugging Face catalog per `(agency, dataset)` pair.

## Repository layout

This is a [`uv` workspace](https://docs.astral.sh/uv/) monorepo. One repository, N PyPI distributions:

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
    │   ├── scripts/           # HF catalog publisher (gated by [publish] extra)
    │   ├── eval/              # retrieval evaluation harness
    │   └── tests/
    └── ...
```

## Building Catalogs

Connectors that ship a curated HuggingFace dataset (the SDMX series
catalog, BLS surveys, Treasury fiscal data, central-bank macro
indicators, ...) include a provider-owned `scripts/build_catalog.py`.
Building is an operator workflow: the user-facing package surface remains
`CONNECTORS`, while the script enumerates rows, builds indexes, and
optionally calls ``await catalog.save(...)`` for local paths or ``hf://...`` uploads.

| Platform | Default location |
|---|---|
| Linux   | `~/.cache/parsimony/catalogs/<provider>/<namespace>/` |
| macOS   | `~/Library/Caches/parsimony/catalogs/<provider>/<namespace>/` |
| Windows | `%LOCALAPPDATA%/parsimony/Cache/catalogs/<provider>/<namespace>/` |

Override the entire cache root with `PARSIMONY_CACHE_DIR` (HF runners,
alternate disks). No per-driver knob — the kernel owns the convention.

```bash
cd packages/sdmx
uv run python scripts/build_catalog.py --catalog series --agency ECB --dataset-id YC --save-root /tmp/sdmx

# Redirect for an HF Space or a faster disk:
PARSIMONY_CACHE_DIR=/data/parsimony \
    uv run python scripts/build_catalog.py --catalog agency --agency ECB --save-root /data/parsimony/sdmx
```

Or push directly with a root URL:

```bash
uv run python scripts/build_catalog.py --catalog agency --agency ECB --push-root hf://parsimony-dev/sdmx
```

Inspect the cache occupancy and find paths via the kernel CLI:

```bash
uv run parsimony cache info
```

Local development:

```bash
uv sync --all-extras --all-packages       # bootstrap the workspace
make verify PKG=fred                      # ruff + mypy + pytest + plugin listing
make verify-all                           # the same, across every package
```

`make verify` mirrors the CI pipeline exactly. If it passes locally, CI passes too.

## Relation to the parsimony ecosystem

This repository is one of three open-source components in the Ockham data stack:

| Repo | PyPI distribution | License | Role |
|---|---|---|---|
| [`parsimony`](https://github.com/ockham-sh/parsimony) | `parsimony-core` | Apache 2.0 | Connector primitives, entry-point discovery, conformance suite |
| **`parsimony-connectors`** (this repo) | `parsimony-<name>` (23 packages) | Apache 2.0 | Officially-maintained provider connectors |
| [`parsimony-agents`](https://github.com/ockham-sh/parsimony-agents) | `parsimony-agents` | Apache 2.0 | Agent loop and orchestration primitives built on the connector layer |

These three libraries form the open-source data-access stack. The fourth component, [ockham (`terminal`)](https://github.com/ockham-sh/terminal) (coming soon), is an AGPLv3 institutional deployment product for self-hosted teams that bundles all three libraries with a web UI and enterprise features.

**Kernel.** `parsimony-core` is a thin shell: connector primitives, entry-point discovery, conformance suite, scaffolding. It knows nothing about specific providers. Connectors depend on the kernel through the stable `parsimony.providers` entry-point contract and a declared contract-version pin, so connector and kernel release cadences are independent.

**Agent framework.** `parsimony-agents` provides the agent loop and tool orchestration that powers the Ockham terminal (coming soon) and can be used standalone in custom agent pipelines. Published to PyPI under Apache 2.0.

Adding a new public data source means adding a new package under `packages/` and passing the conformance suite. The full contract specification lives at [ockham-sh/parsimony `docs/contract.md`](https://github.com/ockham-sh/parsimony/blob/main/docs/contract.md).

## Contributing

- First read: [CONTRIBUTING.md](CONTRIBUTING.md). Local dev workflow, conformance gate, how to add a new connector.
- Governance: [GOVERNANCE.md](GOVERNANCE.md). Acceptance criteria, stewardship, deprecation, graduation.
- Kernel contract: [`parsimony/docs/contract.md`](https://github.com/ockham-sh/parsimony/blob/main/docs/contract.md). The public spec every connector implements.

Anyone may contribute. The conformance suite is the merge gate.

## License

Apache 2.0. Every connector that ships from this repository agrees to Apache 2.0 redistribution. See [GOVERNANCE.md §6](GOVERNANCE.md#6-licence) for how this intersects with third-party provider terms.
