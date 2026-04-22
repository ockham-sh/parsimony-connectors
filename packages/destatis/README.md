# parsimony-destatis

Destatis (German Federal Statistical Office) connector — fetches tables from the GENESIS-Online API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-destatis`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `destatis_fetch` | fetch | Fetch a GENESIS table by `table_id` (e.g. `61111-0001`), with optional `start_year` / `end_year`. German number/date formats are normalized automatically. |
| `enumerate_destatis` | enumerator | Enumerate GENESIS tables via the catalogue API (catalog indexing). |

## Install

```bash
pip install parsimony-destatis
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Both environment variables are **optional**. When unset, the connectors default to the public guest account (`GAST` / `GAST`), which is Destatis's documented anonymous credential pair:

```bash
export DESTATIS_USERNAME="<your-username>"   # optional, defaults to GAST
export DESTATIS_PASSWORD="<your-password>"   # optional, defaults to GAST
```

Register for a personal account at https://www-genesis.destatis.de/genesis/online if the guest account is rate-limited or redirected to an announcement page.

## Quick start

```python
import asyncio
from parsimony_destatis import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["destatis_fetch"](table_id="61111-0001")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://www.destatis.de
- GENESIS-Online: https://www-genesis.destatis.de/genesis/online

## License

See [LICENSE](./LICENSE).
