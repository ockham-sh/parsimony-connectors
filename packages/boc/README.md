# parsimony-boc

Bank of Canada connector — Canadian exchange rates, interest rates, and macroeconomic time series via the Valet API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-boc`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `boc_fetch` | fetch | Fetch one or more BoC time series by name (e.g. `FXUSDCAD,FXEURCAD`) or by group (e.g. `group:FX_RATES_DAILY`). |
| `enumerate_boc` | enumerator | Enumerate every BoC series (~15.6k) and group (~2.4k) via Valet's list endpoints plus a per-group membership fan-out. |
| `boc_search` | search | Semantic/structured search over the published BoC catalog. Pass returned codes to `boc_fetch(series_name=...)`. |

## Install

```bash
pip install parsimony-boc
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the Bank of Canada Valet API is open and unauthenticated.

## Quick start

```python
from parsimony_boc import CONNECTORS

result = CONNECTORS["boc_fetch"](series_name="FXUSDCAD")
print(result.data.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

This plugin currently exposes connectors only. If a catalog is added, it should be a lazy `Catalog` declaration that maintainers build and push directly.

## Provider

- Homepage: https://www.bankofcanada.ca
- API docs: https://www.bankofcanada.ca/valet/docs

## License

See [LICENSE](./LICENSE).
