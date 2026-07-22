# Provider dossier: U.S. Treasury (`treasury`)

> One file per provider/system. Compiled before/while re-running `packages/treasury`
> through the guidebook process (DEEP exploration). Status legend: 🔲 not started ·
> 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked.

**Provider:** U.S. Department of the Treasury — Bureau of the Fiscal Service (Fiscal Data) + Office of Debt Management (interest-rate statistics)
**Key:** `treasury`  ·  **Homepage:** <https://fiscaldata.treasury.gov>
**Distribution:** `parsimony-treasury`  ·  **Namespace(s):** `treasury`
**Kind:** public-keyless
**Status:** ✅ verified-live  ·  **Owner:** espinet  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- **What it serves:** U.S. federal fiscal data (debt, receipts, spending, securities,
  auctions, exchange rates, certified interest rates) via the **Fiscal Data JSON API**,
  plus the Office of Debt Management's **daily interest-rate statistics** (par yield
  curve, real yield curve, bill rates, long-term rates) as OData/Atom **XML** feeds.
- **Auth:** none — fully keyless (both hosts). No `secrets=`/`bind`/`UnauthorizedError`.
- **Discovery model:** build a catalog (no native search). **Two enumeration sources:**
  Fiscal Data is **archetype A** (the live `/services/dtg/metadata/` JSON lists the whole
  universe in one call — self-tracking), the ODM feeds are **archetype D** (a curated
  5-feed registry — the dropdown of interest-rate datasets, which is stable).
- **Total addressable universe (live-counted 2026-06-09):** **56 datasets / 183 endpoint
  stubs / 2,987 fields** in the Fiscal Data metadata → **879 measure fields across 180
  fetchable endpoints / 53 datasets** + **35 ODM rate-feed benchmark rows** = **914 catalog
  entries**. (3 datasets are static-file-only with no JSON API → not cataloguable.)
- **Connectors shipped:** `treasury_fetch`, `treasury_rates_fetch`, `enumerate_treasury`,
  `treasury_search` (4).
- **Completeness verdict:** catalog covers ALL **YES** (live metadata, self-tracking) ·
  connectors cover ALL **PARTIAL by design** (two documented exclusions below).
- **Known gaps / deliberate exclusions:**
  1. **3 static-file-only datasets** (Monthly Treasury Disbursements, Combined Statement,
     Account of Receipts and Expenditures) — no `endpoint_txt`, no JSON API → unfetchable
     via the Fiscal Data API by construction.
  2. **Treasury Coupon Issues + HQM Corporate Bond Yield Curve** — a separate **binary
     `.xls`** product (5-year archives, monthly, Pension-Protection-Act actuarial use), NOT
     part of the daily XML feed family. **Already covered by the FRED connector**
     (`HQMCB20YR` etc. live in FRED). Deferred (FRED-covered + binary transport; the
     alpha_vantage→FRED defer precedent).

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Notes |
|---|--------|-----|------|-------|
| 1 | Fiscal Data API docs | <https://fiscaldata.treasury.gov/api-documentation/> | api-docs | endpoint/param grammar; `filter`/`sort`/`page[size]`; `format=json` |
| 2 | Fiscal Data datasets index | <https://fiscaldata.treasury.gov/datasets/> | web | SPA; Akamai-walls generic UAs (403); consumes the metadata endpoint below |
| 3 | **Dataset metadata endpoint** | `https://api.fiscaldata.treasury.gov/services/dtg/metadata/` | **machine-readable** | **the authoritative enumeration** — 1.2 MB JSON list of every dataset → apis → fields |
| 4 | ODM interest-rate statistics | <https://home.treasury.gov/.../interest-rates/TextView> | web | the 5-option dataset dropdown = the authoritative ODM feed list |
| 5 | ODM XML feed | `https://home.treasury.gov/.../interest-rates/pages/xml?data=<feed>&field_tdr_date_value=<year>` | bulk | OData/Atom XML, paginated by calendar year |
| 6 | Coupon-issue / HQM corporate bond | <https://home.treasury.gov/data/treasury-coupon-issues-and-corporate-bond-yield-curves> | web | **binary `.xls`** 5-year archives (`hqm_24_28.xls`); separate product; in FRED |
| 10 | Terms / public domain | US federal works | legal | public domain; a derived id+title catalog is fine |

### 1.2 The data model

- **Fiscal Data — atomic fetchable unit = an *endpoint* (a table).** Identified by a
  versioned path `v{1,2}/<group>/<sub>/<table>` (e.g. `v2/accounting/od/debt_to_penny`).
  Hierarchy: **dataset → apis[] (endpoints) → fields[]**. Each endpoint returns rows; each
  numeric field is a time-series *measure*. The catalog code is `{endpoint}#{column_name}`
  (compound — one searchable entry per addressable measure).
- **ODM — atomic unit = a (feed, maturity) pair.** Identified `home/<feed>#<column>`
  (e.g. `home/daily_treasury_yield_curve#BC_10YEAR`). Fetched per-feed per-year.
- **No series-id addressing.** Unlike EIA's `/v2/seriesid/{id}`, every Treasury fetch is
  by endpoint path; there is **no out-of-tree fetch surface** (checked live).

### 1.4 The "full universe" question

- **Authoritative enumeration path (Fiscal Data):** `GET /services/dtg/metadata/` — a single
  call returns the **entire** dataset/endpoint/field tree (the same source the fiscaldata
  SPA consumes). **Archetype A** — self-tracking; a new dataset appears automatically.
- **Authoritative enumeration path (ODM):** the interest-rate-statistics page dropdown,
  which lists **exactly 5** datasets. Stable; **archetype D** (curated). The *fields* per
  feed (maturities) are cross-validated against the live feed columns (`harvest_rate_feeds.py`).
- **Pagination / recursion:** none for the metadata call (one GET). The per-endpoint
  *fetch* paginates by `page[size]`; the ODM feeds paginate by year.
- **Total count (live, 2026-06-09):** 56 datasets, 183 endpoint stubs (180 with a queryable
  `endpoint_txt`), 2,987 fields → **879 measure fields**. ODM: 5 feeds → 35 benchmark rows
  (registry == live 2025 column union, exactly).
- **Things NOT in the enumeration (the treasury-trap, checked):**
  - **3 datasets carry no `endpoint_txt`** (static-file/PDF publications) → no JSON API.
  - The **HQM corporate-bond / coupon-issue** family (binary `.xls`, in FRED) — §0 exclusion 2.
- **Gated behind a plan/login:** none (keyless, public domain).

---

## 2. Authentication & access

- **Auth required? No.** Both `api.fiscaldata.treasury.gov` and `home.treasury.gov` are
  keyless and public-domain. No env var, no `secrets=`, no `bind`/`load(api_key=)`, no
  `UnauthorizedError` on the data path. `load(*, catalog_url=None)` binds only the catalog URL.
- **Rate limits:** none documented; the metadata endpoint + the 5 daily feeds tolerate the
  enumerate fan-out. Integration tests need no secrets (nothing to leak →
  no `assert_no_secret_leak`).

---

## 3. Transport & quirks

- **Base URLs:** `https://api.fiscaldata.treasury.gov/services/api/fiscal_service` (data),
  `https://api.fiscaldata.treasury.gov/services/dtg` (metadata),
  `https://home.treasury.gov/resource-center/.../interest-rates/pages/xml` (ODM feeds).
- **Formats:** Fiscal Data = JSON (`fetch_json`). ODM = **OData/Atom XML** → cannot use
  `fetch_json`; raw `HttpClient.request("GET", op_name=...)` + `check_status` (the §6
  `get_text` shape), then `ElementTree` parse.
- **Numeric coercion (fetch):** coerce only the columns the API's `meta.dataTypes` types as
  `CURRENCY*`/`NUMBER`/`PERCENTAGE*` (prefix match — live `meta.dataTypes` returns *base*
  types, but prefix-match is used for safety), comma-stripped. Never blanket-coerce.
- **Date normalization (ODM):** the time column is `NEW_DATE` (par/real curves),
  `INDEX_DATE` (bills), or `QUOTE_DATE` (long-term) — first-present-wins into a uniform
  `record_date` (ordered tuple), sorted ascending.
- **Trailing-slash 301:** the bare `.../xml/` path 301-redirects; target the slash-free
  `.../pages/xml`. (Host split from path.)
- **No anti-bot on the data paths** (plain httpx; only the *datasets SPA page* is Akamai-walled).

---

## 4. Catalog plan

- **Strategy:** enumerator + catalog_build (single bundle, namespace `treasury`).
- **Code scheme:** `{endpoint}#{column_name}` (Fiscal Data) / `home/{feed}#{column}` (ODM).
  The `home/` prefix + a `source` METADATA column route a search hit to the right fetch
  verb (`fiscal_data` → `treasury_fetch`, `treasury_rates` → `treasury_rates_fetch`).
- **Entity shape:** KEY=`code` (ns `treasury`), TITLE=`title`, METADATA=[description, source,
  endpoint, field, data_type, dataset, category, frequency, earliest_date, latest_date].
  The prose column is named **`description`** (not Fiscal Data's `definition`) so
  `discovery_indexes` indexes it — a column named `definition` would never be searched.
- **Enumeration code:** `enumerate_treasury` GETs `/services/dtg/metadata/`, walks
  datasets → apis → measure-fields (one row each), then appends the static ODM rows.
- **Index policy:** `discovery_indexes()` — `code` BM25, `title`/`description` hybrid
  (BM25 + vector).
- **Catalog URL:** `hf://parsimony-dev/treasury` · env `PARSIMONY_TREASURY_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `treasury_search` | @connector (make_local_search_connector) | yes | discovery | whole catalog (914) |
| `treasury_fetch` | @connector | no | fetch any Fiscal Data endpoint | 180 endpoints |
| `treasury_rates_fetch` | @connector | no | fetch one ODM feed/year (XML) | 5 feeds |
| `enumerate_treasury` | @enumerator | no | catalog feed | whole universe |

- **Deliberately NOT wrapped:** the 3 static-file datasets (no JSON API) and the binary
  `.xls` HQM/coupon-issue family (FRED-covered) — §0 exclusions.

## 6. Output schemas

- **`treasury_fetch`:** KEY=`endpoint` (ns `treasury`), TITLE=`title`, DATA=`record_date`
  (datetime); endpoint-specific columns fold in as DATA.
- **`treasury_rates_fetch`:** KEY=`feed` (ns `treasury`), TITLE=`title`, DATA=`record_date`;
  native rate columns fold in as DATA.
- **`treasury_search`:** code/title/score (+ `source` dispatch in the catalog metadata).

## 7. Tests

- `test_treasury_connectors.py` — offline respx (fetch happy path + Empty/Parse/Invalid;
  XML parse + record_date normalization; enumerate one-row-per-measure + TCIR STRING-rate +
  ODM-rows-land + **registry-shape (35 rows, 1.5-month + 6-week present)**).
- `test_error_mapping_treasury.py` — `ErrorMappingSuite` (keyless, `env_key=None`).
- `test_public_surface.py` — `__all__`, count 4, internals not re-exported.
- `test_build_catalog.py` — index types + default_field + dispatch metadata.
- `catalog_tests/queries.yaml` — `code:` probes (debt_to_penny measure + ODM 10y) + lexical.
- `test_integration_treasury.py` — live: debt_to_penny, yield curve, bill rates, enumerate,
  bounded fixture search + **a live ODM field cross-check** (registry maturities present).

---

## 8. Live verification log (2026-06-09)

| Check | Expected | Actual (live) | Verdict | Action |
|-------|----------|---------------|---------|--------|
| metadata endpoint authoritative | lists whole universe | 56 datasets / 183 endpoints / 2,987 fields, 1.2 MB JSON | ✅ | archetype A |
| measure-field count | "all measures" | 879 (CURRENCY*/NUMBER/PERCENTAGE* + 26 TCIR STRING-rate) | ✅ | catalog = 879+34 |
| sampled endpoints resolve | all fetchable | 18/18 → HTTP 200 + data | ✅ | no catalogued-but-404 |
| out-of-tree fetch path | (EIA-style trap) | none — every fetch by `endpoint_txt`, no series-id | ✅ | — |
| zero-measure datasets | — | 3 static-file-only (no `endpoint_txt`) | ✅ | documented exclusion |
| ODM feed list | 5 hardcoded | dropdown = exactly 5, names match 5/5 | ✅ | archetype D confirmed |
| ODM registry columns | registry == live | registry == live **2025** column union, exactly (par 14, real 5, bill 14) | ✅ | cross-check test + harvester |
| `BC_1_5MONTH` "phantom?" | (my grep said 0) | **present in 2025** — a real CMT point added 2025 (sparse; OData omits nulls per-entry; grep "6WK" had false-matched "26WK") | ⚠️ self-corrected | **kept** (registry was current) |
| `RATE` measure prefix | a real data_type | **never appears** in 2,987 fields | ❌ refuted | **dropped dead prefix** |
| catalog prose searchable? | indexed | `definition` column **not** indexed (policy indexes `description`) → only `title` searched | ❌ refuted | **renamed `definition`→`description`** |
| fetch numeric coercion | suffixed types | `meta.dataTypes` returns base types; prefix-match safe | ✅ | unified to prefix-match |
| HQM corporate bond | XML feed? | **binary `.xls`** 5-yr archives; in FRED | ⚠️ | documented exclusion (FRED) |
| debt_to_penny fetch | tens of trillions | real coerced float > 1e13 | ✅ | — |
| par yield-curve fetch | real 10y rate | BC_10YEAR real, record_date sorted | ✅ | — |

**Completeness sign-off:** the catalog contains all **914** addressable units (879 live
Fiscal Data measures across 180 endpoints + 35 ODM benchmark rows), verified by counting
the live `/services/dtg/metadata/` tree and the 5-feed dropdown, and by cross-checking the
ODM registry against the live 2025 feed column union (exact match); the connectors fetch
every catalogued unit (18/18 sampled endpoints + 5/5 feeds live). Documented exclusions: 3
static-file datasets (no JSON API) and the binary `.xls` HQM/coupon-issue family (FRED-
covered). Signed: espinet, 2026-06-09.

## 9. Open questions / follow-ups

- [ ] Publish the catalog snapshot (`build_catalog.py --push hf://parsimony-dev/treasury`,
      ~914 entries single bundle) — maintainer step, not run.
- [ ] If the user wants the HQM corporate-bond / coupon-issue curves natively (not via FRED),
      add an `.xls`-archive fetch verb (xlrd, the rba xls-hist recipe).
