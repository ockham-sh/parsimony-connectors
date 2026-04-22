# parsimony-snb

Swiss National Bank connector plugin for parsimony — monetary, exchange-rate, balance-of-payments, and price series from the SNB data portal (https://data.snb.ch).

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-snb`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `snb_fetch` | connector | Fetch an SNB cube as a tidy DataFrame by `cube_id`, with optional date window, dimension selection, and language. |
| `enumerate_snb` | enumerator | Enumerate the curated SNB cube list with inferred category and frequency (drives the `snb` catalog). |

## Install

```bash
pip install parsimony-snb
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
import asyncio
from parsimony_snb import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["snb_fetch"](cube_id="rendoblim")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Catalog publishing

This plugin publishes a catalog under the `snb` namespace, backed by `enumerate_snb` (param-less; probes a curated SNB cube list).

```bash
parsimony publish --provider snb --target "hf://<your-org>/parsimony-snb"
```

The `{namespace}` placeholder in `--target` is substituted with `snb` at publish time; targets support `file://`, `hf://`, and `s3://` schemes.

## Provider

- Homepage: <https://www.snb.ch>
- Data portal: <https://data.snb.ch>

## License

See [LICENSE](./LICENSE).
