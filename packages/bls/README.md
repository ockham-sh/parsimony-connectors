# parsimony-bls

US Bureau of Labor Statistics connector — labor market, employment, inflation (CPI), and producer price time series.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bls`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bls_fetch` | fetch | Fetch a single BLS time series by `series_id` between two years. |
| `enumerate_bls` | enumerator | Enumerate popular BLS series across all surveys via the `/surveys` and `/timeseries/popular` endpoints. |

## Install

```bash
pip install parsimony-bls
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

The API key is **optional** — the plugin works without one but registering gives you higher rate limits.

```bash
export BLS_API_KEY="<your-key>"   # optional
```

Register for a free key at https://data.bls.gov/registrationEngine/.

## Quick start

```python
import asyncio
from parsimony_bls import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    # LNS14000000 = US unemployment rate (seasonally adjusted)
    result = await connectors["bls_fetch"](
        series_id="LNS14000000",
        start_year="2020",
        end_year="2024",
    )
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://www.bls.gov
- Developer portal: https://www.bls.gov/developers/
- Series browser: https://data.bls.gov/

## License

See [LICENSE](./LICENSE).
