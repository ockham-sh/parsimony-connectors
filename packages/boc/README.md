# parsimony-boc

Bank of Canada connector — Canadian exchange rates, interest rates, and macroeconomic time series via the Valet API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-boc`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `boc_fetch` | fetch | Fetch one or more BoC time series by name (e.g. `FXUSDCAD,FXEURCAD`) or by group (e.g. `group:FX_RATES_DAILY`). |
| `enumerate_boc` | enumerator | Enumerate all BoC series (15,000+) via `/lists/series/json`. |

## Install

```bash
pip install parsimony-boc
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the Bank of Canada Valet API is open and unauthenticated.

## Quick start

```python
import asyncio
from parsimony_boc import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["boc_fetch"](series_name="FXUSDCAD")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Catalog publishing

This plugin publishes catalogs under the `boc` namespace. Build and push:

```bash
parsimony publish --provider boc --target "hf://<your-org>/parsimony-boc"
```

## Provider

- Homepage: https://www.bankofcanada.ca
- API docs: https://www.bankofcanada.ca/valet/docs

## License

See [LICENSE](./LICENSE).
