# parsimony-fred

**FRED (Federal Reserve Economic Data) connector for the [parsimony](https://github.com/ockham-sh/parsimony) framework.**

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-fred`.

## What it does

Once installed, this plugin is discovered automatically and exposes the following connectors:

| Connector | Kind | Tool-tagged | Description |
|---|---|---|---|
| `fred_search` | connector | yes | Keyword search across FRED series (id, title, units, frequency). |
| `fred_fetch` | connector | — | Fetch observation-level data for a FRED series by `series_id`. |

## Install

```bash
pip install parsimony-fred
```

Pulls in a compatible `parsimony-core` automatically.

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
from parsimony_fred import CONNECTORS

result = CONNECTORS["fred_fetch"](series_id="UNRATE")
print(result.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalog discovery

FRED ships a native search endpoint, so this plugin does **not** publish an
embedded catalog. Use `fred_search` for keyword discovery — it calls
`/series/search` directly against the FRED API.

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
