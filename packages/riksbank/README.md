# parsimony-riksbank

Sveriges Riksbank source for parsimony: time-series fetch and catalog enumeration via the SWEA API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-riksbank`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `riksbank_fetch` | connector | Fetch a single Riksbank time series by `series_id` (e.g. `SEKEURPMI`). Returns date + value with the series title. |
| `enumerate_riksbank` | enumerator | Enumerate every Riksbank series via the `/Groups` and `/Series` endpoints with frequency and group-path metadata. |

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
