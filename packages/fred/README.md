# parsimony-fred

**FRED (Federal Reserve Economic Data) connector for the [parsimony](https://github.com/ockham-sh/parsimony) framework.**

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-fred`.

## What it does

Once installed, this plugin is discovered automatically and exposes the following connectors:

| Connector | Kind | Tool-tagged | Description |
|---|---|---|---|
| `fred_search` | connector | yes | Keyword search across FRED series (id, title, units, frequency). |
| `fred_fetch` | connector | — | Fetch observation-level data for a FRED series by `series_id`. |
| `enumerate_fred` | enumerator | — | Enumerate every FRED series across every release (catalog build). |
| `enumerate_fred_release` | enumerator | — | Enumerate all series in a single FRED release. |

## Install

```bash
pip install parsimony-fred
```

Pulls in `parsimony-core>=0.4,<0.5` automatically.

## Configuration

```bash
export FRED_API_KEY="<your-key>"
```

Get a key at <https://fred.stlouisfed.org/docs/api/api_key.html>.

Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
import asyncio
from parsimony_fred import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["fred_fetch"](series_id="UNRATE")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

To expose tool-tagged connectors over MCP, install the standalone server: <https://github.com/ockham-sh/parsimony-mcp>.

## Catalog publishing

This plugin publishes a catalog under the `fred` namespace, backed by `enumerate_fred` (param-less; walks every FRED release).

```bash
parsimony publish --provider fred --target "hf://<your-org>/parsimony-fred"
```

## Development

```bash
uv sync --extra dev
uv run pytest
```

Release-blocking conformance test: `uv run pytest tests/test_conformance.py`.

## Provider

- Homepage: <https://fred.stlouisfed.org>
- API docs: <https://fred.stlouisfed.org/docs/api/fred/>

## License

Apache-2.0 — see [LICENSE](./LICENSE).
