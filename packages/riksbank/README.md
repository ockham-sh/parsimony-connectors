# parsimony-riksbank

Sveriges Riksbank (Sweden's central bank) connector plugin for parsimony. Surfaces **all
five** of the Riksbank's public REST APIs: interest & exchange rates (SWEA), the Swedish
Krona Short-Term Rate (SWESTR), Monetary Policy forecasts & outcomes, market Turnover
Statistics, and securities Holdings. **Keyless** — no API credentials required.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors)
monorepo. Distributed standalone on PyPI as `parsimony-riksbank`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `riksbank_fetch` | connector | Fetch a SWEA interest/exchange-rate series by id (e.g. `SEKEURPMI`), windowed or latest. |
| `riksbank_swestr_fetch` | connector | Fetch a SWESTR series — the overnight fixing, a compounded average (1W–6M) or the index. |
| `riksbank_monetary_policy_fetch` | connector | Fetch a Monetary Policy forecast/outcome series (e.g. `SEQGDPNAYSA`); pick one `policy_round` vintage or get all. |
| `riksbank_turnover_fetch` | connector | Fetch aggregated turnover for a market (`fi`/`fx`/`ird`) at a frequency (`daily`/`monthly`). |
| `riksbank_holdings_fetch` | connector | Fetch the Riksbank's securities holdings (`swedish_securities` per-ISIN, or `swedish_securities_aggregated`). |
| `enumerate_riksbank` | enumerator | Enumerate every addressable unit across all five products for catalog indexing. |
| `riksbank_search` | connector | Semantic-search the published `riksbank` catalog and return matching `code` + `title` + `score`. |

## Install

```bash
pip install parsimony-riksbank
```

Pulls in a compatible `parsimony-core` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
from parsimony_riksbank import CONNECTORS

# The Riksbank's GDP forecast (annual % change), latest policy round.
result = CONNECTORS["riksbank_monetary_policy_fetch"](
    series="SEQGDPNAYSA",
    policy_round="2026:1",
)
print(result.data.head())
```

## Catalogs

This plugin ships a `riksbank` catalog (~156 entries) driven by `enumerate_riksbank`.
`riksbank_search` loads a published snapshot (overridable via the
`PARSIMONY_RIKSBANK_CATALOG_URL` env var) and falls back to building one in-process when no
snapshot is reachable. Maintainers build and push the snapshot with `scripts/build_catalog.py`.

The catalog code routes the follow-up fetch:

- bare SWEA id (e.g. `SEKEURPMI`) → `riksbank_fetch(series_id=...)`
- bare SWESTR id (e.g. `SWESTR`, `SWESTRAVG1M`) → `riksbank_swestr_fetch(series=...)`
- `monetary_policy/<id>` → `riksbank_monetary_policy_fetch(series=<id>)`
- `turnover/<market>/<frequency>` → `riksbank_turnover_fetch(market=, frequency=)`
- `holdings/<dataset>` → `riksbank_holdings_fetch(dataset=<dataset>)`

## Coverage

The Riksbank publishes **five** public REST APIs (confirmed against the developer portal's
API list); this plugin covers all five.

Catalog covers ALL: **yes** for the series-shaped products — SWEA `/Series` and Monetary
Policy `/forecasts/series_ids` are authoritative live enumerations, and SWESTR / Turnover /
Holdings have small, stable dimensions enumerated as registries. Connectors cover ALL
accessible data: **yes** — every product has a fetch verb. Notes on deliberate scope:

- **SWESTR `PRESWESTR`** (preliminary 2021 test-period values) is excluded as superseded
  test data.
- **SWEA derived endpoints** — `CrossRates`, `ObservationAggregates`, `CalendarDays` — are
  *computed conveniences* over the same 117 series (cross-rate calculation, resampling,
  banking-day lookups), not new addressable series, so they are not catalogued.
- **Turnover** is catalogued at the `(market, frequency)` dataset granularity; the
  asset/contract/counterparty facets are returned *inside* each dataset. The Excel-report
  endpoints duplicate the JSON content and are not wrapped.

## Authentication & rate limits

All five products are **open / keyless**. An optional `Ocp-Apim-Subscription-Key` (set
`RIKSBANK_API_KEY` or pass `api_key=`) only raises the keyless quota of **5 requests/minute,
1000/day per IP** — recommended for full catalog builds.

## Provider

- API portal: <https://developer.api.riksbank.se/apis>
- Statistics: <https://www.riksbank.se/en-gb/statistics/>

## License

See [LICENSE](./LICENSE).
