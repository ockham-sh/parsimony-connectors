# parsimony-bdp

Banco de Portugal connector — Portuguese macroeconomic, monetary, and financial time series via the BPstat API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bdp`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bdp_fetch` | fetch | Fetch a Banco de Portugal time series by domain ID + dataset ID (with optional series filter and date range). |
| `enumerate_bdp` | enumerator | Enumerate BdP domains, datasets, and paginated series across all 65 leaf domains. |
| `bdp_search` | tool | Semantic-search the published BdP BPstat catalog snapshot for series/dataset/domain codes. |

## Install

```bash
pip install parsimony-bdp
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the BPstat API is open and unauthenticated.

## Quick start

```python
from parsimony_bdp import CONNECTORS

# Discover available datasets first via enumerate_bdp, then fetch:
result = CONNECTORS["bdp_fetch"](domain_id=1, dataset_id="<dataset_id>")
print(result.data.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: https://www.bportugal.pt
- BPstat portal: https://bpstat.bportugal.pt
- API docs: https://bpstat.bportugal.pt/data/docs

## License

See [LICENSE](./LICENSE).
