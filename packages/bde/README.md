# parsimony-bde

Banco de España connector — Spanish macroeconomic, monetary, and financial time series via the BIEST REST API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bde`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bde_fetch` | fetch | Fetch one or more BdE time series by series code (comma-separated). |
| `enumerate_bde` | enumerator | Enumerate BdE series by querying well-known series codes for catalog seeding. |

## Install

```bash
pip install parsimony-bde
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the BdE BIEST API is open and unauthenticated.

## Quick start

```python
import asyncio
from parsimony_bde import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["bde_fetch"](key="D_1NBAF472")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Catalog publishing

This plugin publishes catalogs under the `bde` namespace. Build and push:

```bash
parsimony publish --provider bde --target "hf://<your-org>/parsimony-bde"
```

## Provider

- Homepage: https://www.bde.es
- API docs: https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html
- Series browser: https://app.bde.es/bie_www/bie_wwwias/xml/Arranque.html (BIEST)

## License

See [LICENSE](./LICENSE).
