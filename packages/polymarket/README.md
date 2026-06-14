# parsimony-polymarket

Polymarket source for parsimony: prediction-market discovery and prices via the public Gamma and CLOB HTTP APIs.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-polymarket`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `polymarket_markets` | enumerator | Discover Polymarket markets via the Gamma API (`id` + `question`, with `slug`/`active` metadata). Takes `limit` (1-100) and `active`. |
| `polymarket_events` | enumerator | Discover Polymarket events via the Gamma API (`id` + `title`, with `slug` metadata). Takes `limit` (1-100). |
| `polymarket_market_prices` | connector | Fetch the current CLOB buy-side price for one outcome token (`token_id`). Returns a raw price dict, e.g. `{"price": "0.5"}`. |

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

markets = CONNECTORS["polymarket_markets"](limit=5, active=True)
print(markets.data.head())

events = CONNECTORS["polymarket_events"](limit=5)
print(events.data.head())

# token_id comes from a market's `clobTokenIds` field on the Gamma API.
price = CONNECTORS["polymarket_market_prices"](token_id="<clob-token-id>")
print(price.data)
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
