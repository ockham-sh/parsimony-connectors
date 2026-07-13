# Provider dossier: U.S. SEC EDGAR (`sec_edgar`)

> One file per provider/system. This is the **single place** where ALL of the
> provider's documentation, API behaviour, and our findings are compiled before
> a line of connector code is written.
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** U.S. Securities and Exchange Commission — EDGAR  ·  **Key:** `sec_edgar`  ·  **Homepage:** <https://www.sec.gov/edgar>
**Distribution:** `parsimony-sec-edgar`  ·  **Namespace(s):** `sec_edgar`
**Kind:** public-keyless (required non-secret `User-Agent` header)
**Status:** ✅ verified-live  ·  **Owner:** connectors sweep  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR (fill last)

- What this provider serves, in one sentence: every public filing submitted to the U.S. SEC (10-K/10-Q/8-K/Form 4/13F/S-1/…), the registrant metadata behind them, the documents inside them, and the XBRL-tagged financial facts extracted from them.
- Auth: **none** (no API key). SEC's fair-access policy *requires* a `User-Agent` header (name + contact email); env var `SEC_EDGAR_USER_AGENT`. Non-secret infra header, not `secrets=`/`bind`.
- Discovery model: **native search** (EDGAR full-text search at `efts.sec.gov`, 2001→present, over filing *content*) + the published ticker map for the ~10.4k exchange-listed issuers. **No catalog** (decision tree → wrap native search). `sec_edgar` is in `EXCLUDED_COMMERCIAL_PROVIDERS`.
- Total addressable universe: ~800k+ historical filers / **tens of millions of filings** (no single JSON endpoint enumerates them; full enumeration is the quarterly `full-index` archive). XBRL facts cover forms first required in 2009. (How counted: the JSON APIs are per-entity; the universe figure is the `full-index` archive + the FTS 2001-content corpus, not a single count.)
- Connectors shipped (7): `full_text_search`, `find_company`, `submissions`, `fetch_filing`, `company_concept`, `company_facts`, `frames`.
- Completeness verdict: discovery covers ALL? **YES** — `full_text_search` is native search over all filers' content since 2001 (+ ticker-map fast path). · connectors cover ALL accessible data classes? **YES** — registrant, filing (full history via `include_older`), document (any age via `index.json`), and XBRL fact three ways (per-company history, all-facts, cross-company frame). Deliberate exclusions: the bulk ZIPs + `full-index` crawl (publish/ETL, not on-demand).
- Known gaps / deliberate exclusions: bulk ZIP archives (companyfacts.zip / submissions.zip) and the `full-index` crawl are out of scope for an on-demand connector (they are publish/ETL tooling, not agent fetches).

---

## 1. Documentation compilation (THE MOST IMPORTANT STEP)

### 1.1 Source inventory

| # | Source | URL | Type | Last seen current? | Notes |
|---|--------|-----|------|--------------------|-------|
| 1 | EDGAR Application Programming Interfaces | <https://www.sec.gov/search-filings/edgar-application-programming-interfaces> | api-docs | 2026-06-09 (page "Last Updated April 8, 2025") | The authoritative reference for the `data.sec.gov` RESTful APIs: submissions, companyconcept, companyfacts, frames. Quoted below. |
| 2 | Accessing EDGAR Data (fair-access / programmatic) | <https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data> | ops/legal | 2026-06-09 | The fair-access policy: **10 requests/second**, the required `User-Agent`, `Accept-Encoding: gzip, deflate`, the `index.{html,xml,json}` per-directory crawl helpers, and the CIK/ticker association files. Quoted below. |
| 3 | EDGAR Full-Text Search (frontend) | <https://efts.sec.gov/LATEST/search-index?q=…> | api (undocumented JSON) | 2026-06-09 | The JSON API behind <https://www.sec.gov/cgi-bin/srqsb> / the EFTS UI. Searches filing **content** 2001→present. No official schema page; shape captured live in §8. |
| 4 | CIK / ticker / exchange association files | `/files/company_tickers.json`, `/files/company_tickers_exchange.json`, `/files/company_tickers_mf.json` | bulk | 2026-06-09 | `company_tickers.json` = 10,400 `{cik_str,ticker,title}` (exchange-listed issuers only). The `_mf` variant adds fund series/class. |
| 5 | Bulk-download archives | `/Archives/edgar/daily-index/xbrl/companyfacts.zip`, `/Archives/edgar/daily-index/bulkdata/submissions.zip`, `/Archives/edgar/full-index/` | bulk | 2026-06-09 | Nightly-rebuilt ZIPs of the whole API corpus; `full-index/{year}/QTR{n}/` is the authoritative per-quarter enumeration of *every* filing. Publish/ETL tooling, not an on-demand fetch. |
| 6 | Per-directory crawl helpers | `…/index.json` in any `/Archives` directory | api | 2026-06-09 | Every Archives dir (incl. each accession folder) carries an `index.json` listing its files — the robust way to resolve a filing's documents without trusting `primaryDocument`. |
| 8 | Existing client libraries | `sec-edgar-downloader`, `edgartools`, `python-edgar`, `sec-api` (commercial) | code | 2026-06-09 | De-facto references for endpoint lists + the UA requirement; all wrap the same `data.sec.gov` + `efts.sec.gov` + `/Archives` surface. |
| 10 | Terms / fair access | source #2 + Privacy & Security Policy / Developer FAQs | legal | 2026-06-09 | "These APIs do not require any authentication or API keys." Public-domain U.S. government data; redistribution permitted with fair-access compliance. No catalog to redistribute (native search). |

**Verbatim, load-bearing (source #1):**

> "data.sec.gov" was created to host RESTful data APIs delivering JSON-formatted data … These APIs do not require any authentication or API keys to access. Currently included in the APIs are the submissions history by filer and the XBRL data from financial statements (forms 10-Q, 10-K, 8-K, 20-F, 40-F, 6-K, and their variants).

> Each entity's current filing history is available at `https://data.sec.gov/submissions/CIK##########.json` … The object's property path contains at least one year's of filing or to 1,000 (whichever is more) of the most recent filings in a compact columnar data array. **If the entity has additional filings, `files` will contain an array of additional JSON files and the date range for the filings each one contains.**

> The company-concept API returns all the XBRL disclosures from a single company (CIK) and concept (a taxonomy and tag) into a single JSON file, with a separate array of facts for each units on measure … `…/api/xbrl/companyconcept/CIK##########/us-gaap/AccountsPayableCurrent.json`

> This API returns all the company concepts data for a company into a single API call: `…/api/xbrl/companyfacts/CIK##########.json`

> The xbrl/frames API aggregates one fact for each reporting entity that is last filed that most closely fits the calendrical period requested … `…/api/xbrl/frames/us-gaap/AccountsPayableCurrent/USD/CY2019Q1I.json` … The period format is `CY####` for annual data (duration 365 days +/- 30 days), `CY####Q#` for quarterly data (duration 91 days +/- 30 days), and `CY####Q#I` for instantaneous data.

**Verbatim, load-bearing (source #2):**

> Current max request rate: **10 requests/second.** … Please declare your user agent in request headers: `User-Agent: Sample Company Name AdminContact@<sample company domain>.com` / `Accept-Encoding: gzip, deflate` / `Host: www.sec.gov`

### 1.2 The data model (in the provider's own terms)

EDGAR has **four** distinct atomic units — this is why it is "not the typical timeseries connector":

1. **Registrant / entity** — identified by a 10-digit zero-padded **CIK**. Carries name, former names, SIC, tickers, exchanges, addresses, EIN/LEI, category. Discoverable by ticker (the map, 10.4k) or by any field via full-text search (all filers).
2. **Filing** — identified by an **accession number** (`0000320193-24-000123`). Has a form type, filing date, report (period) date, and a set of documents. A filer's filings are the `submissions` columnar arrays.
3. **Filing document** — a file inside an accession folder on `/Archives` (HTML/XML/txt/pdf). The `primaryDocument` names the main one (may point to an XSL *viewer* subpath, e.g. `xslF345X06/form4.xml`, vs the raw `form4.xml` at the folder root).
4. **XBRL fact** — a numeric financial disclosure tagged `(taxonomy, concept/tag, unit, period)`, e.g. `us-gaap:AccountsPayableCurrent / USD / FY2023`. Aggregated three ways: per-company-per-concept history (`companyconcept`), all-concepts-for-a-company (`companyfacts`), and one-concept-one-period-across-all-companies (`frames`).

### 1.3 Endpoint reference (verbatim, one row per endpoint) — all live-confirmed §8

| Endpoint | Host | Method | Params | Returns | Pagination |
|----------|------|--------|--------|---------|------------|
| `/submissions/CIK##########.json` | data.sec.gov | GET | CIK (10-digit) | entity metadata + `filings.recent` (16-col columnar arrays, ≤1000 rows) + `filings.files[]` | older filings in `filings.files[].name` → `/submissions/<name>.json` |
| `/api/xbrl/companyconcept/CIK##########/{taxonomy}/{tag}.json` | data.sec.gov | GET | CIK, taxonomy (`us-gaap`/`dei`/`ifrs-full`/`srt`), tag | `{cik,taxonomy,tag,label,description,entityName,units:{<unit>:[{end,val,accn,fy,fp,form,filed,start?,frame?}]}}` | none (one company's full history) |
| `/api/xbrl/companyfacts/CIK##########.json` | data.sec.gov | GET | CIK | `{cik,entityName,facts:{<taxonomy>:{<tag>:{label,description,units:{…}}}}}` (large, multi-MB) | none |
| `/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json` | data.sec.gov | GET | taxonomy, tag, unit (`USD`, `USD-per-shares`, `pure`…), period (`CY####`/`CY####Q#`/`CY####Q#I`) | `{taxonomy,tag,ccp,uom,label,description,pts,data:[{accn,cik,entityName,loc,start?,end,val}]}` | none (one cross-section; `pts` = entity count) |
| `/files/company_tickers.json` | www.sec.gov | GET | — | `{idx:{cik_str,ticker,title}}` (10,400) | none |
| `/Archives/edgar/data/{cik}/{accession_nodash}/index.json` | www.sec.gov | GET | cik, accession | `{directory:{name,item:[{name,type,size,last-modified}]}}` | none |
| `/Archives/edgar/data/{cik}/{accession_nodash}/{document}` | www.sec.gov | GET | — | raw document body (HTML/XML/txt) | none |
| `/LATEST/search-index` | efts.sec.gov | GET | `q` (required), `forms`, `dateRange=custom`+`startdt`+`enddt`, `ciks`, `category`, `from` (offset) | ElasticSearch envelope: `{hits:{total:{value,relation},hits:[{_id,_score,_source:{ciks,display_names,form,root_forms,file_date,period_ending,adsh,sics,biz_locations,…}}]}}` | `from` offset, **100 hits/page**, `total.value` caps at 10,000 (`relation:gte`) |

### 1.4 The "full universe" question — how is EVERYTHING listed?

- **Authoritative discovery path:** there is **no** single JSON endpoint that lists all ~800k+ filers. Discovery is native and threefold: (a) **EDGAR full-text search** (`efts`) over filing *content* 2001→present — the broad search surface across every filer; (b) the **ticker map** (`company_tickers.json`, 10.4k) — fast exact ticker/CIK/name lookup for exchange-listed issuers; (c) the **`full-index` archive** (`/Archives/edgar/full-index/{year}/QTR{n}/`) — the only *exhaustive* enumeration of every filing ever, by quarter (bulk/ETL, not an agent fetch).
- **Pagination / recursion:** FTS pages by `from` (100/page, 10k window). Submissions paginate older filings via `filings.files[]`.
- **Estimated total:** SEC does not publish a single "N filers" count via the APIs; the FTS corpus alone returns `total.relation:gte 10000` on broad queries. The universe is "every public filing since 1994 (full-text since 2001)."
- **Treasury-trap surfaces** (exist but not in the obvious endpoint): the `companyconcept`/`frames` XBRL APIs are addressable only by `(taxonomy, tag, …)` — not discoverable from the submissions tree; the valid concept tags come from the us-gaap/dei taxonomies (a `companyfacts` call enumerates the tags a given company actually reports).
- **Gated/unreachable:** none — everything is public, keyless. Only rate-limited (10 req/s).

**Conclusion: native search ⇒ NO catalog.** Q1 (discovery completeness) is answered by wrapping `efts` full-text search (all filers, by content) alongside the ticker map (fast exact lookup). The ticker map *alone* (today's only discovery verb) covers ~1.3% of filers and cannot search content.

---

## 2. Authentication & access

- Auth required? **No API key.** Mechanism: a required, **non-secret** `User-Agent` request header identifying the requester (name + contact email). SEC rejects generic/missing UAs with 403/429 (confirmed live: WebFetch's default UA → HTTP 403).
- Env var: `SEC_EDGAR_USER_AGENT` (e.g. `"Acme Research contact@acme.com"`). Resolved before any network call; fast-fail `UnauthorizedError("sec_edgar", env_var="SEC_EDGAR_USER_AGENT")` when unset.
- Modelled as a header (never a query param ⇒ not logged/redacted), so deliberately **not** via `secrets=`/`bind`/`load`. There is no secret to strip.
- Rate limits: **10 requests/second** (fair access). `Accept-Encoding: gzip, deflate` requested. No documented daily quota.
- ⛔ **Human intervention?** No *key* needed, but live tests `require_env("SEC_EDGAR_USER_AGENT")` and **skip** (→ ship-broken risk, guidebook §8 rule 5) until the var is set. **The var is currently absent from `ockham/.env`** — so this connector is presently UNVERIFIED-LIVE in CI, exactly like the eia precedent. *Ask the user whether to add it (their email as the SEC contact identity).*

---

## 3. Transport & quirks

- Base URLs (three): `https://data.sec.gov` (submissions + XBRL JSON), `https://www.sec.gov` (ticker maps + `/Archives` document bodies + `index.json`), `https://efts.sec.gov` (full-text search). `data.sec.gov` **404s** the `/Archives` path — keep the hosts separate (documented in code).
- Response formats: JSON everywhere except filing document bodies (HTML/XML/txt → `_get_text`).
- `fetch_json` handles all the JSON APIs (canonical status semantics). No non-canonical status mapping needed.
- **No Akamai / TLS-fingerprint wall** — plain `httpx` + a proper `User-Agent` reaches all three hosts (confirmed live). **No `curl_cffi`.**
- Pagination: submissions `filings.files[]`; FTS `from` offset (100/page, 10k window).
- **FTS date gotcha:** passing `startdt`/`enddt` **without** `dateRange=custom` returns **HTTP 500** (confirmed live). Always send `dateRange=custom` when supplying explicit dates.
- **FTS has no date sort — results are relevance-ranked.** Probed live (2026-06-09): `sort=date` → **HTTP 500**; `sort=filed`/`sort=newest`/`sortby=filed` → HTTP 200 but **byte-identical ordering** (silently ignored). So `full_text_search` returns by `_score` (relevance); "most recent" must be done client-side (bound dates, sort returned rows by `filing_date`). Documented in the verb's docstring rather than faked with a misleading client-side `sort=` arg.
- CIK normalization: strip non-digits, `zfill(10)` for the JSON APIs; the `/Archives` *path* uses the **un-padded** integer CIK and the **dash-stripped** accession; the full-submission `.txt` filename uses the **dashed** accession. (Three different CIK/accession spellings in one fetch path.)
- `primaryDocument` may be an XSL **viewer** subpath (`xslF345X06/form4.xml`); the raw doc sits at the accession-folder root. `index.json` is the source of truth for what files exist.

---

## 8. Live verification log (documentation is a claim; execution is the truth)

All probes 2026-06-09, throwaway UA `parsimony-connectors research espinetandreu@gmail.com`, ≤3 req/s.

| Check | Expected (docs) | Actual (live) | Verdict |
|-------|-----------------|---------------|---------|
| generic UA rejected | 403/429 | WebFetch default UA → **HTTP 403**; proper UA → 200 | ✅ confirms UA mandate |
| `company_tickers.json` shape/size | ticker→CIK map | dict, **10,400** entries `{cik_str,ticker,title}` (e.g. NVDA→1045810) | ✅ |
| submissions `filings.recent` cap | ≤1000 recent | Apple `recent`=**1000 rows**, 16 cols; `files`=[`…-001.json`, 1234 more, 1994–2015] | ✅ pagination real |
| submissions prolific filer | additional files | JPMorgan `recent`=**24,806**, **67** additional files (~2000 each) ⇒ ~158k filings | ✅ |
| **submissions completeness bug** | should reach all filings | current `submissions`/`fetch_filing` read **only `recent`** → old filings unresolvable | ⚠️ **bug** |
| companyconcept | per-company concept history | Apple `AccountsPayableCurrent` → `units.USD` = **140 facts** `{end,val,accn,fy,fp,form,filed}` | ✅ unwrapped |
| frames | one concept, one period, all cos | `CY2019Q1I/AccountsPayableCurrent/USD` → `pts`=**3,390** `{accn,cik,entityName,loc,end,val}` | ✅ unwrapped |
| full-text search | content search, all filers | `q="climate risk"&forms=10-K` → `hits.total`=1446; `_source` rich; `_id`=`{accn}:{file}` | ✅ unwrapped (the native search) |
| FTS paging | offset paging | `from=10` → 100 hits/page; broad `q` → `total.relation:gte 10000` | ✅ |
| FTS dates | start/end filter | `startdt/enddt` w/o `dateRange=custom` → **HTTP 500**; with it → 200, 698 hits | ⚠️ gotcha |
| FTS sort | a date-sort param | `sort=date` → **HTTP 500**; `sort=filed`/`newest`, `sortby=filed` → 200 but **ordering unchanged** (ignored) | ⚠️ relevance-only; documented in docstring |
| Archives `index.json` | document listing | accession dir → `directory.item[]` (`form4.xml` 5404B + index files) | ✅ robust doc resolution |

**Build's own live run (2026-06-09, all 7 verbs, throwaway UA, via the integration suite):**

| Verb | Live result | Verdict |
|------|-------------|---------|
| `full_text_search` | `q="climate risk"&forms=10-K` → non-empty, every hit has accession + document, form starts `10-K` | ✅ |
| `find_company` | `AAPL` → CIK `0000320193`, title contains APPLE | ✅ |
| `submissions` | Apple, 5-col schema incl `reportDate`, real accessions/forms/dates | ✅ |
| **`submissions` `include_older`** | Apple `form=10-K`: with_older **> recent-only count**, oldest filingDate **< 2010** | ✅ **completeness fix proven** |
| `fetch_filing` | Apple latest 10-K → primary doc resolved from `index.json` ends `.htm`, body > 1000 chars | ✅ |
| `company_concept` | Apple `Assets` → long frame, `val` max > $1e10, USD facts | ✅ |
| `company_facts` | Apple → dict, cik 320193, us-gaap populated | ✅ |
| `frames` | `AccountsPayableCurrent/USD/CY2019Q1I` → **>1000** filers, cik 10-digit padded | ✅ |
| `find_company` no-match | bogus ticker → `EmptyDataError` | ✅ |

**Completeness sign-off:** discovery is native and complete (full-text search over all filers' content 2001→present; ticker map as a fast exact path), and the connectors expose every accessible data class (registrant / filing / document / XBRL fact in three aggregations), with the bulk-ZIP + `full-index` archives the only documented exclusion (publish/ETL, not on-demand). Signed: connectors sweep on 2026-06-09. Gate: ruff + mypy (13 files) + 61 offline + 9 live integration + `parsimony list --strict`, all green.

---

## 5. Connector plan (DECIDED 2026-06-09: comprehensive — 7 verbs)

| Connector | Decorator | Tool? | Purpose | Slice of the universe |
|-----------|-----------|-------|---------|------------------------|
| `sec_edgar_full_text_search` | @connector | yes | **NEW** — native content search (efts) | all filers, 2001→present, by keyword/form/date/CIK |
| `sec_edgar_find_company` | @connector | yes | fast exact ticker/CIK/name lookup | the 10.4k exchange-listed issuers (ticker map) |
| `sec_edgar_submissions` | @connector | yes | list a filer's filings (newest-first) + `form` filter + `include_older` page-walk | a CIK's filings (recent window, or all via `include_older`) |
| `sec_edgar_fetch_filing` | @connector | yes | one document body; resolves the primary doc via `index.json` for ANY accession | a filing's documents |
| `sec_edgar_company_concept` | @connector | yes | **NEW** — one concept's full history for one company (tidy long timeseries) | per-company XBRL fact series |
| `sec_edgar_company_facts` | @connector | yes | raw XBRL facts blob (all concepts) | a company's full XBRL set |
| `sec_edgar_frames` | @connector | yes | **NEW** — one concept, one period, all reporting companies | cross-company XBRL snapshot |

- **No enumerator / catalog** — native search (efts) is the discovery surface; stays in `EXCLUDED_COMMERCIAL_PROVIDERS`.
- Endpoints deliberately NOT wrapped: bulk ZIPs (`companyfacts.zip`/`submissions.zip`) and the `full-index` quarterly crawl — publish/ETL tooling, not on-demand agent fetches.

## 6. Output schemas

- `full_text_search`: KEY=`accession` (ns sec_edgar), TITLE=`display_name`, DATA=`form`,`filing_date`(datetime),`cik`,`document`,`period_ending`(datetime),`score`(numeric). `document` lets the agent chain straight into `fetch_filing(cik, accession, document)`.
- `find_company`: KEY=`cik` (ns sec_edgar), TITLE=`title`, DATA=`ticker`. (unchanged shape)
- `submissions`: KEY=`accessionNumber` (ns sec_edgar), DATA=`filingDate`(datetime),`form`,`primaryDocument`,`reportDate`(datetime).
- `company_concept`: DATA-only tidy long frame — `end`(datetime),`val`(numeric),`unit`,`fy`,`fp`,`form`,`filed`(datetime),`accn`,`start`(datetime). Concept identity (cik/taxonomy/tag) rides in provenance params.
- `frames`: KEY=`cik` (ns sec_edgar), TITLE=`entityName`, DATA=`val`(numeric),`end`(datetime),`loc`,`accn`,`start`(datetime).
- `company_facts`, `fetch_filing`: return a raw `dict` (no OutputSpec), as today.

## 7. Tests

- `test_sec_edgar_connectors.py` — offline, respx + patches at real module paths (the boj/rba seam playbook); every verb happy path + Empty/Parse/InvalidParameter guards + parametrized missing-UA fast-fail over ALL 7 verbs + count guard + the FTS `dateRange=custom` date rule.
- `test_public_surface.py` — NEW: `__all__`, exact `CONNECTORS` count (7), internal seams not re-exported.
- `test_integration_sec_edgar.py` — live, env-gated; all 7 verbs assert REAL content (Apple CIK 320193 + a `frames` cross-section + a `q="climate risk"` FTS hit).
- `test_conformance.py` — `assert_plugin_valid` (kept).
- No catalog ⇒ no `catalog_tests/queries.yaml`, no registry spec (excluded-commercial).

## 8b. Decisions log

- **2026-06-09 — scope = comprehensive (7 verbs)** (user). Adds full-text search + company_concept + frames; fixes submissions (`include_older` page-walk) and fetch_filing (`index.json` resolution, which also sidesteps the XSL-viewer-subpath trap and removes the submissions dependency for old accessions).
- **2026-06-09 — `SEC_EDGAR_USER_AGENT` → `ockham/.env`** (user will write `SEC_EDGAR_USER_AGENT=espinetandreu@gmail.com` themselves). I verify out-of-band with a throwaway UA until that lands.

## 9. Open questions / follow-ups

- [x] Connector scope — comprehensive (7 verbs). · [x] UA in `.env` — user adds it.
- [ ] Live-verify all 7 verbs once code lands (§8 table to be extended with the build's own run).
