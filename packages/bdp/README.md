# parsimony-bdp

Banco de Portugal connector — Portuguese macroeconomic, monetary, and financial time series via the BPstat API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bdp`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bdp_fetch` | fetch | Fetch a Banco de Portugal time series by domain ID + dataset ID (with optional series filter and date range). |
| `enumerate_bdp` | enumerator | Enumerate BdP datasets across all leaf domains (~216 datasets across 77 domains). |

## Install

```bash
pip install parsimony-bdp
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the BPstat API is open and unauthenticated.

## Quick start

```python
import asyncio
from parsimony_bdp import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    # Discover available datasets first via enumerate_bdp, then fetch:
    result = await connectors["bdp_fetch"](domain_id=1, dataset_id="<dataset_id>")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://www.bportugal.pt
- BPstat portal: https://bpstat.bportugal.pt
- API docs: https://bpstat.bportugal.pt/data/docs

## License

See [LICENSE](./LICENSE).
