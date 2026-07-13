# parsimony-eia

U.S. Energy Information Administration (EIA) connector — fetches energy data (petroleum, electricity, natural gas, coal, renewables) from the EIA v2 Open Data API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-eia`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `eia_search` | tool | Semantic-search the dataset catalog; returns a dataset `route` to pass to `eia_fetch`. |
| `eia_fetch` | fetch | Fetch a dataset by `route` (e.g. `petroleum/pri/spt`) with optional `measure`, `facets`, `frequency`, `start`, `end`. Paged in full. |
| `eia_fetch_series` | fetch | Fetch by a legacy APIv1 series id (e.g. `PET.RWTC.D`) via the `/v2/seriesid/{id}` path. |
| `eia_facets` | tool | List the valid `{id, name}` values of a dataset's facet dimension, to narrow a fetch to a series. |
| `enumerate_eia` | enumerator | Walk the v2 route tree to one row per leaf dataset (the catalog feed). |

## Coverage

The catalog indexes **all 232 leaf datasets** across EIA's 14 top-level
categories (petroleum, natural gas, electricity, coal, nuclear, renewables/
densified-biomass, total energy, emissions, international, SEDS, STEO, AEO, IEO,
crude-oil imports), each carrying its measure and facet-dimension manifest. EIA's
full series universe (~2M series) is the facet cartesian product of those
datasets — too large to catalog individually, but every series is **fetchable**:
by `route` + `facets` filters, or by its legacy series id via `eia_fetch_series`.
So discovery is at the dataset tier and access is total.

EIA caps every data response at 5,000 rows; the fetch connectors page through to
completeness automatically (a request matching more than 300,000 rows is refused
with guidance to narrow it). Data is U.S. federal public domain — cite as
"Source: U.S. Energy Information Administration".

## Install

```bash
pip install parsimony-eia
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export EIA_API_KEY="<your-key>"
```

Get a free key at https://www.eia.gov/opendata/register.php.

## Quick start

```python
from parsimony_eia import CONNECTORS

result = CONNECTORS["eia_fetch"](route="petroleum/pri/spt", frequency="monthly")
print(result.raw.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: https://www.eia.gov
- API docs: https://www.eia.gov/opendata/documentation.php

## License

See [LICENSE](./LICENSE).
