# parsimony-bdf

Banque de France connector — French macroeconomic, monetary, and financial time series via the SDMX-based Webstat API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bdf`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bdf_fetch` | fetch | Fetch a Banque de France SDMX time series by key (e.g. `EXR.M.USD.EUR.SP00.E`). |
| `enumerate_bdf` | enumerator | Enumerate all BdF datasets via the SDMX catalogue endpoint. |

## Install

```bash
pip install parsimony-bdf
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export BANQUEDEFRANCE_KEY="<your-key>"
```

Register for a free key at https://developer.webstat.banque-france.fr/. The key is sent via the `X-IBM-Client-Id` header (IBM API Connect gateway).

## Quick start

```python
import asyncio
from parsimony_bdf import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["bdf_fetch"](key="EXR.M.USD.EUR.SP00.E")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://www.banque-france.fr
- Webstat portal: https://webstat.banque-france.fr
- Developer portal: https://developer.webstat.banque-france.fr/

## License

See [LICENSE](./LICENSE).
