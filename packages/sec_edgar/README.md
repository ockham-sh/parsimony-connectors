# parsimony-sec-edgar

SEC EDGAR connector plugin for parsimony — company lookup, recent filings, XBRL company facts, and raw filing documents for U.S. issuers over the public `data.sec.gov` / `www.sec.gov` APIs.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-sec-edgar`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `sec_edgar_find_company` | connector | Resolve an SEC registrant by ticker symbol or CIK from the published ticker map. Returns cik + ticker + company title. |
| `sec_edgar_submissions` | connector | List recent filings for a CIK (accession number, filing date, form type, primary document). |
| `sec_edgar_company_facts` | connector | Raw XBRL company-facts blob for a CIK (all reported concepts keyed by taxonomy). |
| `sec_edgar_fetch_filing` | connector | Fetch one filing document body from the EDGAR archives (resolves the primary document when none is given). |

## Install

```bash
pip install parsimony-sec-edgar
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## User-Agent (required)

SEC's fair-access policy **requires** every request to carry a `User-Agent` header that identifies the requester (a name and contact email). A generic or missing User-Agent gets a `403`/`429`. There is no API key — the User-Agent is the only thing you must supply, via the `SEC_EDGAR_USER_AGENT` environment variable:

```bash
export SEC_EDGAR_USER_AGENT="Acme Research contact@acme.com"
```

If it is unset, the connectors fail fast with `UnauthorizedError` naming the env var, before any network call.

## Quick start

```python
from parsimony_sec_edgar import CONNECTORS

result = CONNECTORS["sec_edgar_find_company"](identifier="AAPL")
print(result.data.head())
```

For multi-plugin composition:

```python
from parsimony import discover
connectors = discover.load_all()
```

## Provider

- Homepage: <https://www.sec.gov>
- EDGAR: <https://www.sec.gov/edgar>
- Fair-access policy: <https://www.sec.gov/os/accessing-edgar-data>

## License

See [LICENSE](./LICENSE).
