# parsimony-sec_edgar

SEC EDGAR connector plugin for parsimony — public-company filings, financial statements (XBRL), filing documents, and insider trades for U.S. issuers.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-sec_edgar`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `sec_edgar_find_company` | connector | Search EDGAR by name, ticker, or CIK. Returns CIK, name, ticker. |
| `sec_edgar_company_profile` | connector | Profile for one company: name, CIK, ticker, industry, SIC, fiscal year end. |
| `sec_edgar_income_statement` | connector | Income statement from 10-K/10-Q XBRL — multi-period summary or single-filing detailed view. |
| `sec_edgar_balance_sheet` | connector | Balance sheet from 10-K/10-Q XBRL — multi-period summary or detailed view. |
| `sec_edgar_cashflow_statement` | connector | Cash flow statement from 10-K/10-Q XBRL — multi-period summary or detailed view. |
| `sec_edgar_search_filings` | connector | Full-text search across all EDGAR filings, optionally scoped by form and date. |
| `sec_edgar_filings` | connector | List filings for a company or across all companies, filtered by form and date. |
| `sec_edgar_company_facts` | connector | All XBRL company facts for a company (custom time-series base). |
| `sec_edgar_filing_document` | connector | Filing content as markdown by accession number. |
| `sec_edgar_filing_metadata` | connector | Form-specific metadata summary for a filing. |
| `sec_edgar_filing_sections` | connector | Table of contents (item identifiers + titles) for a filing. |
| `sec_edgar_filing_item` | connector | Specific section/item of a filing as text. |
| `sec_edgar_filing_tables` | connector | List tables in a filing with caption, type, and shape. |
| `sec_edgar_filing_table` | connector | Specific table from a filing as a DataFrame. |
| `sec_edgar_insider_trades` | connector | Structured insider transactions (Form 4) for a company. |

## Install

```bash
pip install parsimony-sec_edgar
```

Pulls in `parsimony-core>=0.4,<0.5` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Quick start

```python
import asyncio
from parsimony_sec_edgar import CONNECTORS

async def main():
    connectors = CONNECTORS.bind_env()
    result = await connectors["sec_edgar_find_company"](identifier="AAPL")
    print(result.data.head())

asyncio.run(main())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all().bind_env()
```

EDGAR requires a user-agent identity per SEC fair-access policy. Set `EDGAR_IDENTITY` (or `SEC_EDGAR_USER_AGENT`) to a string of the form `"YourApp your-email@example.com"` before issuing requests.

## Provider

- Homepage: <https://www.sec.gov>
- EDGAR: <https://www.sec.gov/edgar>
- Fair-access policy: <https://www.sec.gov/os/accessing-edgar-data>

## License

See [LICENSE](./LICENSE).
