# parsimony-tiingo

Tiingo connector plugin for parsimony — equities, crypto, and forex prices, company metadata, fundamentals, and news.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-tiingo`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| **Discovery** | | |
| `tiingo_search` | connector | Search Tiingo for stocks, ETFs, mutual funds, and crypto by name or ticker. |
| **Equities** | | |
| `tiingo_eod` | connector | End-of-day OHLCV with split/dividend-adjusted columns. Free tier: full history. |
| `tiingo_iex` | connector | Real-time IEX top-of-book quotes (composite last, OHLV, bid/ask, sizes). |
| `tiingo_iex_historical` | connector | Historical IEX intraday OHLC at a given resample frequency (last ~2000 points). |
| `tiingo_meta` | connector | Equity metadata: name, description, exchange, listing dates. |
| **Fundamentals** | | |
| `tiingo_fundamentals_meta` | connector | Sector, industry, SIC, currency, location, SEC filing link, ADR flag. |
| `tiingo_fundamentals_definitions` | connector | All fundamental metric definitions (dataCode, name, statement type, units). |
| **News** | | |
| `tiingo_news` | connector | News articles filtered by tickers, source, date range. Power+ plan only. |
| **Crypto** | | |
| `tiingo_crypto_prices` | connector | Historical crypto OHLCV at multiple resample frequencies. |
| `tiingo_crypto_top` | connector | Real-time top-of-book quotes for crypto pairs. |
| **Forex** | | |
| `tiingo_fx_prices` | connector | Historical forex OHLC at multiple resample frequencies. |
| `tiingo_fx_top` | connector | Real-time top-of-book forex quotes (mid, bid/ask, sizes). |
| **Enumeration** | | |
| `enumerate_tiingo` | enumerator | Supported-tickers list (~127k rows) for catalog indexing. |

## Install

```bash
pip install parsimony-tiingo
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

```bash
export TIINGO_API_KEY="<your-key>"
```

Get a key at <https://www.tiingo.com/account/api/token>.

## Quick start

```python
import asyncio
from parsimony_tiingo import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["tiingo_eod"](ticker="AAPL")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: <https://www.tiingo.com>
- API docs: <https://www.tiingo.com/documentation/general/overview>

## License

See [LICENSE](./LICENSE).
