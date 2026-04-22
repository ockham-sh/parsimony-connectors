# parsimony-financial-reports

FinancialReports.eu connector — European company filings (annual reports, interim reports, ESEF) plus company, ISIN, and ISIC reference data via the official SDK.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-financial-reports`.

## Connectors

10 connectors grouped by capability:

| Name | Kind | Description |
|---|---|---|
| `fr_companies_search` | fetch | Search/list companies by name, country, ISIN, ticker, or ISIC industry code. |
| `fr_company_retrieve` | fetch | Full company profile by internal ID. |
| `fr_filings_search` | fetch | Search filings by company, type, date, country, language, and more. |
| `fr_filing_retrieve` | fetch | Full filing metadata by ID. |
| `fr_filing_markdown` | fetch | Full filing content as markdown text (Level 2 API access). |
| `fr_filing_history` | fetch | Audit trail of changes to a filing. |
| `fr_next_annual_report` | fetch | Predict a company's next annual report date with a confidence score. |
| `fr_isic_browse` | fetch | Browse ISIC industry classifications (sections / divisions / groups / classes). |
| `fr_isin_lookup` | fetch | Look up ISINs with OpenFIGI enrichment (FIGI, security type, exchange). |
| `fr_reference_data` | fetch | List reference data: filing types, categories, languages, countries, sources. |

## Install

```bash
pip install parsimony-financial-reports
```

Pulls in `parsimony-core>=0.4,<0.5` and the official `financial-reports-generated-client` SDK automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

Set the following environment variable:

```bash
export FINANCIAL_REPORTS_API_KEY="<your-key>"
```

Request a key at https://financialreports.eu.

## Quick start

```python
import asyncio
from parsimony_financial_reports import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["fr_companies_search"](countries="DE", page_size=10)
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

## Provider

- Homepage: https://financialreports.eu
- API docs: https://financialreports.eu/developers

## License

See [LICENSE](./LICENSE).
