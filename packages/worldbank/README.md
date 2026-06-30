# parsimony-worldbank

[World Bank API](https://api.worldbank.org/v2) connector for the [parsimony](https://github.com/ockham-sh/parsimony) framework.

The World Bank API v2 is a **keyless** public JSON/XML API — no registration, no
authentication, no rate limiting. It serves development indicators (GDP,
population, trade, employment, education, health, etc.) per country and year.

> **Note:** This package is **not affiliated, endorsed, or supported by the World
> Bank Group.** It is an independent community connector built against the public
> World Bank API.

## Features

-   **Keyless access** — no `api_key` parameter, no `load()` arguments.
-   Fetch one indicator for one country (or all countries) as a pandas DataFrame.
-   Automatic pagination — up to 10 000 observations per call.
-   Standard parsimony connector interface via `CONNECTORS` / `load()`.

## `worldbank_fetch`

Parsimony connector that fetches World Bank indicator observations.

```python
from parsimony_worldbank import load

connectors = load()
result = connectors["worldbank_fetch"](
    indicator_id="NY.GDP.MKTP.CD",  # GDP (current US$)
    country="USA",                   # or "all" for every country
    date_from=2010,
    date_to=2020,
)
# Returns a DataFrame with columns:
#   indicator_id, indicator_name, country, country_iso3, date, value
```

### Parameters

| Parameter      | Type   | Default | Description                                      |
|----------------|--------|---------|--------------------------------------------------|
| `indicator_id` | `str`  | —       | World Bank series code (e.g. `NY.GDP.MKTP.CD`).  |
| `country`      | `str`  | `"all"` | ISO 3166-1 alpha-3 code, or `"all"`.             |
| `date_from`    | `str`  | `None`  | Start year (`YYYY`). `None` means no lower bound. |
| `date_to`      | `str`  | `None`  | End year (`YYYY`). `None` means no upper bound.   |

### Output

A `pd.DataFrame` with one row per (country, year) and the following columns:

-   `indicator_id` — the series code used in the request.
-   `indicator_name` — human-readable indicator name.
-   `country` — country name (English).
-   `country_iso3` — ISO 3166-1 alpha-3 country code.
-   `date` — year string (`YYYY`).
-   `value` — numeric observation, or `None` if the value is missing.

## Installation

```bash
pip install parsimony-worldbank
```

Or, from the monorepo:

```bash
pip install -e packages/worldbank
```

## Development

```bash
# Install with dev dependencies
pip install -e "packages/worldbank[dev]"

# Run linter
ruff check packages/worldbank

# Run type checker
mypy packages/worldbank

# Run tests
pytest packages/worldbank/tests
```

---

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo.
