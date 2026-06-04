# parsimony-bde

Banco de España connector — Spanish macroeconomic, monetary, and financial time series via the BIEST REST API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bde`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bde_fetch` | connector | Fetch one or more BdE time series by series code (comma-separated). |
| `enumerate_bde` | enumerator | Discover BdE series by crawling the seven published catalog CSV chapters. |
| `bde_search` | connector | Semantic-search the published BdE catalog snapshot; returns ranked series codes. |

## Install

```bash
pip install parsimony-bde
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No API key required — the BdE BIEST API is open and unauthenticated.

`bde_search` reads a published catalog snapshot (default `hf://parsimony-dev/bde`).
Override the snapshot location with the `PARSIMONY_BDE_CATALOG_URL` environment
variable, or pass `catalog_url=` at call time.

## Quick start

```python
import asyncio
from parsimony_bde import CONNECTORS

async def main():
    connectors = CONNECTORS
    result = await connectors["bde_fetch"](key="D_1NBAF472")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

`enumerate_bde` discovers the full BdE series catalog by crawling the published
CSV chapters; maintainers build a `Catalog` snapshot from it (see
`scripts/build_catalog.py`) and push it to the snapshot URL that `bde_search`
reads. The crawl is expensive (seven chapters; the CF/Financial-Accounts chapter
alone is several thousand series), so it runs offline as a publish job — never at
query time.

## Provider

- Homepage: https://www.bde.es
- API docs: https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html
- Series browser: https://app.bde.es/bie_www/bie_wwwias/xml/Arranque.html (BIEST)

## License

See [LICENSE](./LICENSE).
