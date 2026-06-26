# parsimony-sec-edgar

SEC EDGAR connector plugin for parsimony — full-text search over filing content, company lookup, a filer's filings, raw filing documents, and XBRL financial data (per-company concept history, all-concepts facts, and cross-company frames) for U.S. issuers over the public `efts.sec.gov` / `data.sec.gov` / `www.sec.gov` APIs.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-sec-edgar`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `sec_edgar_full_text_search` | connector | Native EDGAR full-text search over the content of every filing since 2001, across all filers. Filter by form, date range, or CIK; returns the cik + accession + document needed to fetch a hit. |
| `sec_edgar_find_company` | connector | Fast exact lookup of a registrant by ticker symbol or CIK from the published ticker map (the ~10.4k exchange-listed issuers). Returns cik + ticker + company title. |
| `sec_edgar_submissions` | connector | List a CIK's filings (newest first): accession number, filing date, form type, primary document, report date. `form` filters to one form type; `include_older` walks the additional pages so the full history is reachable. |
| `sec_edgar_fetch_filing` | connector | Fetch one filing document body from the EDGAR archives. Resolves the primary document via the filing's `index.json` (works for any filing, however old). |
| `sec_edgar_company_concept` | connector | One XBRL concept's full reported history for a company, as a tidy long timeseries (period, value, unit, fiscal year/period, form, filed). |
| `sec_edgar_company_facts` | connector | Raw XBRL company-facts blob for a CIK (all reported concepts keyed by taxonomy). |
| `sec_edgar_frames` | connector | One XBRL concept for one period across every reporting company — a cross-sectional snapshot. |

## Coverage

EDGAR is **not** a timeseries provider — it exposes four atomic units (registrant, filing, document, XBRL fact), and the seven connectors cover all of them. **Discovery** is native: `sec_edgar_full_text_search` searches the content of every filing since 2001 across all ~800k+ filers, so there is **no built catalog** (the provider has first-party search). **Fetch** reaches a filer's whole filing history (`include_older`), any document (`index.json` resolution), and the XBRL financial data three ways — per-company concept history, all-concepts facts, and cross-company frames.

Deliberately not wrapped (publish/ETL tooling, not on-demand agent fetches): the nightly bulk ZIP archives (`companyfacts.zip`, `submissions.zip`) and the quarterly `full-index` crawl. Data is U.S. federal public domain; SEC's only requirement is the fair-access `User-Agent` below.

## Install

```bash
pip install parsimony-sec-edgar
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## User-Agent (required)

SEC's fair-access policy (max **10 requests/second**) **requires** every request to carry a `User-Agent` header that identifies the requester (a name and contact email). A generic or missing User-Agent gets a `403`/`429`. There is no API key — the User-Agent is the only thing you must supply, via the `SEC_EDGAR_USER_AGENT` environment variable:

```bash
export SEC_EDGAR_USER_AGENT="Acme Research contact@acme.com"
```

If it is unset, the connectors fail fast with `UnauthorizedError` naming the env var, before any network call.

## Quick start

```python
from parsimony_sec_edgar import CONNECTORS

# Discover filings by content across all filers...
hits = CONNECTORS["sec_edgar_full_text_search"](query="climate risk", forms="10-K")
print(hits.data.head())

# ...or look up a known issuer and read its financials as a timeseries.
rev = CONNECTORS["sec_edgar_company_concept"](cik="320193", tag="Revenues")
print(rev.data.head())
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
