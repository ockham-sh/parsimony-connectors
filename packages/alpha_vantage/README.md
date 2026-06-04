# parsimony-alpha-vantage

Alpha Vantage connector — equities, fundamentals, forex, crypto, precious metals, US economic indicators, technical indicators, and news sentiment.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-alpha-vantage`.

## Connectors

29 connectors total (28 `@connector` + 1 `@enumerator`), grouped by category. See the [Alpha Vantage docs](https://www.alphavantage.co/documentation/) for endpoint-level reference.

| Group | Count | Verbs |
|---|---|---|
| Discovery | 1 | `alpha_vantage_search` |
| Market data (OHLCV) | 5 | `alpha_vantage_quote`, `alpha_vantage_daily`, `alpha_vantage_weekly`, `alpha_vantage_monthly`, `alpha_vantage_intraday` |
| Company fundamentals | 6 | `alpha_vantage_overview`, `alpha_vantage_income_statement`, `alpha_vantage_balance_sheet`, `alpha_vantage_cash_flow`, `alpha_vantage_earnings`, `alpha_vantage_etf_profile` |
| Calendars | 2 | `alpha_vantage_earnings_calendar`, `alpha_vantage_ipo_calendar` |
| Forex | 4 | `alpha_vantage_fx_rate`, `alpha_vantage_fx_daily`, `alpha_vantage_fx_weekly`, `alpha_vantage_fx_monthly` |
| Crypto | 3 | `alpha_vantage_crypto_daily`, `alpha_vantage_crypto_weekly`, `alpha_vantage_crypto_monthly` |
| Economic indicators | 1 | `alpha_vantage_econ` (GDP, CPI, unemployment, fed funds, treasury yield, etc.) |
| Precious metals | 2 | `alpha_vantage_metal_spot`, `alpha_vantage_metal_history` |
| Alpha intelligence | 2 | `alpha_vantage_news`, `alpha_vantage_top_movers` |
| Technical indicators | 1 | `alpha_vantage_technical` (50+ indicators via unified endpoint) |
| Options | 1 | `alpha_vantage_options` (premium plan only) |
| Enumerator | 1 | `enumerate_alpha_vantage` (US listings, active or delisted) |

Total: 28 `@connector` + 1 `@enumerator` = 29.

Commodity series (WTI, Brent, natural gas, copper, etc.) are intentionally omitted — use the FRED connector instead, which has superior historical coverage.

## Install

```bash
pip install parsimony-alpha-vantage
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export ALPHA_VANTAGE_API_KEY="<your-key>"
```

Get a free key at https://www.alphavantage.co/support/#api-key. Free tier: 25 requests/day across all endpoints.

## Quick start

```python
import asyncio
import os
from parsimony_alpha_vantage import load

async def main():
    # load(api_key=...) binds the key across every connector and strips it
    # from provenance (it is declared secrets=("api_key",) on each verb).
    connectors = load(api_key=os.environ["ALPHA_VANTAGE_API_KEY"])
    result = await connectors["alpha_vantage_quote"](symbol="IBM")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: https://www.alphavantage.co
- API docs: https://www.alphavantage.co/documentation/
- Rate limits: free tier 25 requests/day shared across all endpoints (hard cap); the API returns an HTTP 200 `Information` body when the cap is hit, mapped to `RateLimitError`

## License

See [LICENSE](./LICENSE).
