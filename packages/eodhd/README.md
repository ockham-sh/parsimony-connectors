# parsimony-eodhd

EODHD connector — end-of-day, intraday, fundamentals, news, calendars, macro and technical indicators from the EODHD REST API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-eodhd`.

## Connectors

17 connectors grouped by capability:

| Name | Kind | Description |
|---|---|---|
| `eodhd_search` | fetch | Resolve company names / partial tickers to EODHD ticker codes (`AAPL.US`). |
| `eodhd_exchanges` | fetch | List supported exchanges. |
| `eodhd_exchange_symbols` | fetch | List all symbols on an exchange. |
| `eodhd_eod` | fetch | End-of-day OHLCV for a ticker (daily/weekly/monthly). |
| `eodhd_live` | fetch | Live (real-time or 15-min delayed) quote. |
| `eodhd_intraday` | fetch | Intraday OHLCV at 1m / 5m / 1h. |
| `eodhd_bulk_eod` | fetch | EOD prices for every symbol on an exchange in one request. |
| `eodhd_dividends` | fetch | Dividend history for a ticker. |
| `eodhd_splits` | fetch | Stock split history for a ticker. |
| `eodhd_fundamentals` | fetch | Full fundamentals for a stock or ETF (raw nested dict). |
| `eodhd_calendar` | fetch | Earnings / IPO / analyst trends calendars. |
| `eodhd_news` | fetch | Financial news, optionally filtered by ticker. |
| `eodhd_macro` | fetch | Single macro indicator time series for a country. |
| `eodhd_macro_bulk` | fetch | All available macro indicators for a country. |
| `eodhd_technical` | fetch | Technical indicators (SMA, EMA, MACD, BBANDS, ADX, etc.). |
| `eodhd_insider` | fetch | Insider (executive / director) transactions. |
| `eodhd_screener` | fetch | Screen stocks by structured filter triples. |

Several endpoints require paid EODHD plans (EOD+Intraday, Fundamentals); per-connector docstrings tag the minimum plan as `[Free+]`, `[EOD+Intraday+]`, or `[Fundamentals+]`.

## Install

```bash
pip install parsimony-eodhd
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export EODHD_API_KEY="<your-key>"
```

Get a key at https://eodhd.com/register.

## Quick start

```python
import asyncio
from parsimony_eodhd import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["eodhd_eod"](ticker="AAPL.US")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://eodhd.com
- API docs: https://eodhd.com/financial-apis/

## License

See [LICENSE](./LICENSE).
