# parsimony-coingecko

CoinGecko connector — crypto market data via the CoinGecko v3 API and on-chain prices via GeckoTerminal.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-coingecko`.

## Connectors

11 connectors grouped by capability:

| Name | Kind | Description |
|---|---|---|
| `coingecko_search` | fetch | Resolve coin names/symbols to CoinGecko IDs. |
| `coingecko_trending` | fetch | Top 7 trending coins by 24-h search volume. |
| `coingecko_top_gainers_losers` | fetch | Top gaining and losing coins over a window. |
| `coingecko_price` | fetch | Current price(s) for one or more coins in one or more vs-currencies. |
| `coingecko_markets` | fetch | Ranked market data (price, market cap, ATH/ATL, 24h change), paged. |
| `coingecko_coin_detail` | fetch | Full per-coin metadata (description, links, market data) — returns nested dict. |
| `coingecko_market_chart` | fetch | Historical price/market-cap/volume over the last N days. |
| `coingecko_market_chart_range` | fetch | Historical price/market-cap/volume between two ISO dates. |
| `coingecko_ohlc` | fetch | OHLC candlesticks for a coin. |
| `coingecko_token_price_onchain` | fetch | On-chain token price by contract address (GeckoTerminal). |
| `enumerate_coingecko` | enumerator | Full coin list (~17 000 rows) for catalog indexing. |

`coingecko_top_gainers_losers` is PRO-only, and `coingecko_market_chart_range` beyond
365 days is restricted on the Demo plan — both return `PaymentRequiredError` on a Demo key.

## Install

```bash
pip install parsimony-coingecko
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export COINGECKO_API_KEY="<your-key>"
```

Get a Demo key at https://www.coingecko.com/en/api/pricing.

## Quick start

```python
import os
from parsimony_coingecko import load

# Bind the key off the call surface (kept out of provenance and logs).
connectors = load(api_key=os.environ["COINGECKO_API_KEY"])
result = connectors["coingecko_price"](ids="bitcoin", vs_currencies="usd")
print(result.data.head())
```

If the key is not bound, each connector falls back to `COINGECKO_API_KEY` from the
environment, and a missing key fails fast with `UnauthorizedError`.

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: https://www.coingecko.com
- API docs: https://docs.coingecko.com/v3.0.1/reference/introduction

## License

See [LICENSE](./LICENSE).
