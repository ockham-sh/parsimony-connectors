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

19 connectors total. Each docstring opens with the lowest FMP plan that can call it —
`[Basic+]`, `[Starter+]`, `[Premium+]`, `[Ultimate]`, matching FMP's own ladder of Basic
(free) → Starter → Premium → Ultimate. Tags were derived from FMP's plan-comparison matrix
and checked against a live Basic key on 2026-07-23.

Two gates are not endpoint-level, so a reachable endpoint can still refuse a specific call
with `PaymentRequiredError`:

- **Symbol.** On Basic, symbol-scoped endpoints serve only FMP's fixed 87-ticker sample
  ("AAPL, TSLA, AMZN and 84 more"; the list is not published). Starter widens to US
  exchanges, Premium to US/UK/Canada, Ultimate to global. This is why one basket of
  large caps can half-succeed on a single key.
- **Range.** Date-windowed verbs cap the span per plan and reject a wider window rather
  than truncating it.

**Status semantics:** an invalid key returns 401 → `UnauthorizedError`; a plan or legacy restriction returns 403 (FMP also uses 402) → `PaymentRequiredError`. An unknown symbol returns HTTP 200 with `[]` → `EmptyDataError`.

## Install

```bash
pip install parsimony-fmp
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

The API key is supplied per-call via `bind(api_key=...)` / `load(api_key=...)`,
or — as a dev fallback — from the `FMP_API_KEY` environment variable:

```bash
export FMP_API_KEY="<your-key>"
```

Get a key at <https://financialmodelingprep.com>. The key is declared as a
secret (`secrets=("api_key",)`), so it is stripped from recorded provenance and
redacted from logs. A missing key fails fast with `UnauthorizedError`.

## Quick start

```python
from parsimony_fmp import load

connectors = load(api_key="<your-key>")   # or rely on FMP_API_KEY
result = connectors["fmp_quotes"](symbols="AAPL,TSLA,MSFT")
print(result.raw.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: <https://financialmodelingprep.com>
- API docs: <https://site.financialmodelingprep.com/developer/docs>

## License

See [LICENSE](./LICENSE).
