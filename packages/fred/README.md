# parsimony-fred

**FRED (Federal Reserve Economic Data) connector for the [parsimony](https://github.com/ockham-sh/parsimony) framework.**

## What it does

Once installed alongside `parsimony-core`, this plugin is discovered automatically and exposes the following connectors:

| Connector | Kind | Tool-tagged | Description |
|---|---|---|---|
| `fred_search` | search | ✓ | Keyword search across FRED series (id, title, units, frequency). |
| `fred_fetch` | fetch | — | Fetch observation-level data for a FRED series by `series_id`. |
| `enumerate_fred_release` | enumerator | — | Enumerate all series in a FRED release (catalog indexing). |

## Install

```bash
pip install parsimony-core parsimony-fred
export FRED_API_KEY=<your-key>  # https://fred.stlouisfed.org/docs/api/api_key.html
```

Verify discovery:

```bash
parsimony list-plugins
```

## Use

```python
from parsimony.connectors import build_connectors_from_env

connectors = build_connectors_from_env()
result = await connectors["fred_fetch"](series_id="UNRATE")
df = result.data  # pandas DataFrame
```

Or via the MCP server (exposes only tool-tagged connectors):

```bash
parsimony mcp serve --tool-only
```

## Development

```bash
uv sync --extra dev
uv run pytest
```

Release-blocking conformance test: `uv run pytest tests/test_conformance.py`.

## License

Apache-2.0 — see [LICENSE](LICENSE).
