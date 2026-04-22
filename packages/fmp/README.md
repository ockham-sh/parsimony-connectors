# parsimony-fmp

Financial Modeling Prep source for parsimony: discovery, quotes, fundamentals, events, signals, and a global equity screener.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-fmp`.

## Connectors

| Group | Connectors |
|---|---|
| Discovery | `fmp_search`, `fmp_taxonomy` |
| Core market data | `fmp_quotes`, `fmp_prices` |
| Fundamentals | `fmp_company_profile`, `fmp_peers`, `fmp_income_statements`, `fmp_balance_sheet_statements`, `fmp_cash_flow_statements` |
| Events and catalysts | `fmp_corporate_history`, `fmp_event_calendar`, `fmp_analyst_estimates` |
| Signals and context | `fmp_news`, `fmp_insider_trades`, `fmp_institutional_positions`, `fmp_earnings_transcript` |
| Market context | `fmp_index_constituents`, `fmp_market_movers` |
| Screener | `fmp_screener` |

19 connectors total. Tier coverage is annotated per-connector in the docstrings (`[All plans]`, `[Starter+]`, `[Professional+]`); the demo plan returns AAPL/TSLA/MSFT only for symbol-bound endpoints.

## Install

```bash
pip install parsimony-fmp
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

```bash
export FMP_API_KEY="<your-key>"
```

Get a key at <https://financialmodelingprep.com>.

## Quick start

```python
import asyncio
from parsimony_fmp import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["fmp_quotes"](symbols="AAPL,TSLA,MSFT")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: <https://financialmodelingprep.com>
- API docs: <https://site.financialmodelingprep.com/developer/docs>

## License

See [LICENSE](./LICENSE).
