# parsimony-treasury

US Treasury Fiscal Data connector plugin for parsimony — debt, revenue, spending, securities, and other federal fiscal datasets from the Bureau of the Fiscal Service.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-treasury`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `treasury_fetch` | connector | Fetch any Fiscal Data API endpoint as a tidy DataFrame, with optional `filter`, `sort`, and page size. |
| `enumerate_treasury` | enumerator | Enumerate every Treasury Fiscal Data dataset and endpoint for catalog indexing (drives the `treasury` catalog). |

## Install

```bash
pip install parsimony-treasury
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
import asyncio
from parsimony_treasury import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["treasury_fetch"](
        endpoint="v2/accounting/od/debt_to_penny",
        sort="-record_date",
        page_size=10,
    )
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Catalog publishing

This plugin publishes a catalog under the `treasury` namespace, backed by `enumerate_treasury` (param-less; walks the Fiscal Data dataset metadata API).

```bash
parsimony publish --provider treasury --target "hf://<your-org>/parsimony-treasury"
```

The `{namespace}` placeholder in `--target` is substituted with `treasury` at publish time; targets support `file://`, `hf://`, and `s3://` schemes.

## Provider

- Homepage: <https://fiscaldata.treasury.gov>
- API docs: <https://fiscaldata.treasury.gov/api-documentation/>

## License

See [LICENSE](./LICENSE).
