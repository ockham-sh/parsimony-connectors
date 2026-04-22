# parsimony-polymarket

Polymarket source for parsimony: prediction-market quotes via the Gamma and CLOB HTTP APIs.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-polymarket`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `polymarket_gamma_fetch` | connector | Generic fetcher against `https://gamma-api.polymarket.com` (events, markets, series). Supports `expand=markets|outcomes` and `response_path` for nested JSON traversal. |
| `polymarket_clob_fetch` | connector | Generic fetcher against `https://clob.polymarket.com` (order book, prices, midpoints). Same `expand` / `response_path` surface. |

Both connectors take `path`, `method`, and arbitrary upstream query params — additional fields on `PolymarketFetchParams` are forwarded as query string.

## Install

```bash
pip install parsimony-polymarket
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No environment variables required — Polymarket's Gamma and CLOB read APIs are public.

## Quick start

```python
import asyncio
from parsimony_polymarket import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["polymarket_gamma_fetch"](
        path="/events",
        limit=5,
        active=True,
        closed=False,
    )
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: <https://polymarket.com>
- Gamma API: <https://gamma-api.polymarket.com>
- CLOB API: <https://clob.polymarket.com>

## License

See [LICENSE](./LICENSE).
