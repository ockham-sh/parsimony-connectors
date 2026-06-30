# parsimony-worldbank

World Bank Open Data connector for the [parsimony](https://github.com/ockham-sh/parsimony) framework.

Provides access to the **World Bank Indicators API v2** — over 1,400 development
indicators across 200+ economies. The API is **keyless and open** — no
authentication required.

## Connectors

| Verb | Description |
|------|-------------|
| `worldbank_search` | Search for indicator codes by keyword (id/name, case-insensitive) |
| `worldbank_fetch` | Fetch indicator observations by country and indicator code |

### worldbank_search

```python
import parsimony_worldbank

result = parsimony_worldbank.worldbank_search(query="GDP")
# → DataFrame with columns: id, name, source_note, source_organization, unit, source_id
```

### worldbank_fetch

```python
# Fetch GDP (current US$) for all countries in 2024
result = parsimony_worldbank.worldbank_fetch(
    indicator="NY.GDP.MKTP.CD",
    country="all",
    date="2024",
)
# → DataFrame with columns: indicator, country, countryiso3code, date, value, unit, title

# Fetch population for a single country with a date range
result = parsimony_worldbank.worldbank_fetch(
    indicator="SP.POP.TOTL",
    country="US",
    date="2020:2024",
)
```

## Provider

[World Bank Open Data](https://data.worldbank.org) — free and open access to
global development data.

## Data

The World Bank Indicators API provides access to:

- **Indicators**: Over 1,400 development indicators covering economics,
  demographics, education, health, environment, infrastructure, and more.
- **Countries**: 200+ economies, plus regional and income-group aggregates.
- **Frequency**: Annual data, with some indicators updated in real time.
- **Coverage**: Historical data from 1960 onwards for most indicators.

The API is keyless — no API key is required. Rate limits apply per IP
(typically generous for non-commercial use).

## License

Apache-2.0
