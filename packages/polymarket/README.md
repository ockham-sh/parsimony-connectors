# parsimony-polymarket

Polymarket source for parsimony: prediction-market discovery and prices via the public Gamma and CLOB HTTP APIs.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-polymarket`.

## Connectors

The surface follows how a Polymarket question is actually navigated: **search → event → market → price history.**

| Name | Kind | Description |
|---|---|---|
| `polymarket_search_events` | enumerator | Natural-language search over events via Gamma `/public-search` (the only Polymarket endpoint that does server-side text search). Returns `slug` + `title` with `markets_count` and `volume`/`liquidity`/`active`/`closed` metadata. Takes `search_text`, `limit` (1-100), `optimized`. |
| `polymarket_event` | enumerator | The markets inside one event, by event `slug`. One row per market (`market_slug` + `market_question`, with outcome count and volume/liquidity/active/closed). |
| `polymarket_market` | enumerator | The outcomes of one market and their CLOB token ids, by market `slug`. One row per outcome (`clob_token_id` + `outcome`). |
| `polymarket_price_history` | connector | The probability time series for one outcome `token_id` via CLOB `/prices-history`. Returns a tidy `timestamp` × `probability` frame. Takes `interval` (max/1m/1w/1d/6h/1h) and `fidelity` (minutes). |

## Install

```bash
pip install parsimony-polymarket
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No environment variables required — Polymarket's Gamma and CLOB read APIs are public. There is no API key, so this package declares no secrets and no `load(*, api_key=...)` convenience.

## Quick start

```python
from parsimony_polymarket import CONNECTORS

# 1. Search events by topic.
events = CONNECTORS["polymarket_search_events"](search_text="inflation", limit=5)
event_slug = events.data.iloc[0]["slug"]

# 2. Drill into the event's markets.
markets = CONNECTORS["polymarket_event"](slug=event_slug)
market_slug = markets.data.iloc[0]["market_slug"]

# 3. Get the market's outcomes and their CLOB token ids.
outcomes = CONNECTORS["polymarket_market"](slug=market_slug)
token_id = outcomes.data.iloc[0]["clob_token_id"]

# 4. Pull that outcome's probability time series.
history = CONNECTORS["polymarket_price_history"](token_id=token_id, interval="1w")
print(history.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: <https://polymarket.com>
- Gamma API: <https://gamma-api.polymarket.com>
- CLOB API: <https://clob.polymarket.com>

## License

See [LICENSE](./LICENSE).
