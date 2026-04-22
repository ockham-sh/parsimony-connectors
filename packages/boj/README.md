# parsimony-boj

Bank of Japan (BoJ) statistics connector — fetches time series from the BoJ Time-Series Data Search.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-boj`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `boj_fetch` | fetch | Fetch BoJ time series by database (`db`) and series `code`. Max 250 codes per request. |
| `enumerate_boj` | enumerator | Enumerate BoJ series across the 45 known databases (catalog indexing). |

## Install

```bash
pip install parsimony-boj
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No environment variables required. The BoJ API is unauthenticated.

## Quick start

```python
import asyncio
from parsimony_boj import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["boj_fetch"](db="FM08", code="FXERD01")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://www.boj.or.jp
- API manual: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf

## License

See [LICENSE](./LICENSE).
