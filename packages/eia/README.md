# parsimony-eia

U.S. Energy Information Administration (EIA) connector — fetches energy data (petroleum, electricity, natural gas, coal, renewables) from the EIA v2 Open Data API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-eia`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `eia_fetch` | fetch | Fetch dataset by API `route` (e.g. `petroleum/pri/spt`), with optional `frequency`, `start`, `end`. |
| `enumerate_eia` | enumerator | Enumerate top-level EIA API routes for catalog indexing. |

## Install

```bash
pip install parsimony-eia
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export EIA_API_KEY="<your-key>"
```

Get a free key at https://www.eia.gov/opendata/register.php.

## Quick start

```python
import asyncio
from parsimony_eia import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["eia_fetch"](route="petroleum/pri/spt", frequency="monthly")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://www.eia.gov
- API docs: https://www.eia.gov/opendata/documentation.php

## License

See [LICENSE](./LICENSE).
