# parsimony-finnhub

Finnhub source for parsimony: equity quotes, company fundamentals, news, and calendars.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-finnhub`.

## Connectors

| Group | Connectors |
|---|---|
| Discovery | `finnhub_search` |
| Market data | `finnhub_quote` |
| Company | `finnhub_profile`, `finnhub_peers`, `finnhub_recommendation`, `finnhub_earnings`, `finnhub_basic_financials` |
| News | `finnhub_company_news`, `finnhub_market_news` |
| Calendars | `finnhub_earnings_calendar`, `finnhub_ipo_calendar` |
| Enumerator | `enumerate_finnhub` (full US symbol list) |

12 connectors total. Free tier covers all of the above; some adjacent endpoints (`/stock/candle`, `/forex/rates`, `/stock/splits`, `/stock/dividend`, `/stock/price-target`) require a paid Finnhub plan and are not exposed here.

## Install

```bash
pip install parsimony-finnhub
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

```bash
export FINNHUB_API_KEY="<your-key>"
```

Get a key at <https://finnhub.io>. Free tier: 60 calls/min.

## Quick start

```python
import asyncio
from parsimony_finnhub import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["finnhub_quote"](symbol="AAPL")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: <https://finnhub.io>
- API docs: <https://finnhub.io/docs/api>

## License

See [LICENSE](./LICENSE).
