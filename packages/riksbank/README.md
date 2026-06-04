# parsimony-riksbank

Sveriges Riksbank source for parsimony: time-series fetch and catalog enumeration via the SWEA and SWESTR APIs.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-riksbank`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `riksbank_fetch` | connector | Fetch a single Riksbank SWEA time series by `series_id` (e.g. `SEKEURPMI`). Returns date + value with the series title; pass `from_date`/`to_date` together for a window. |
| `riksbank_swestr_fetch` | connector | Fetch a SWESTR fixing / compounded average / index series (`SWESTR`, `SWESTRAVG1W…6M`, `SWESTRINDEX`). Returns date + value plus native SWESTR metadata. |
| `enumerate_riksbank` | enumerator | Enumerate every SWEA series via `/Groups` + `/Series` (one request each, ~117 series) plus the static SWESTR registry, with frequency and group-path metadata. |
| `riksbank_search` | connector | Semantic-search the published Riksbank catalog snapshot; returns `code`/`title`/`score` rows for routing to the fetch verbs. |

## Install

```bash
pip install parsimony-riksbank
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

```bash
export RIKSBANK_API_KEY="<your-key>"   # optional
```

`RIKSBANK_API_KEY` is **optional**: the connector binds and runs against the public SWEA endpoints even when the variable is unset (it defaults to `""` and the `Ocp-Apim-Subscription-Key` header is omitted). Register at <https://developer.api.riksbank.se/> for higher quota.

## Quick start

```python
import asyncio
from parsimony_riksbank import CONNECTORS

async def main():
    connectors = CONNECTORS
    result = await connectors["riksbank_fetch"](series_id="SEKEURPMI")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

This plugin currently exposes connectors only. If a catalog is added, it should be a lazy `Catalog` declaration that maintainers build and push directly.

## Provider

- Homepage: <https://www.riksbank.se>
- API docs: <https://developer.api.riksbank.se/>

## License

See [LICENSE](./LICENSE).
