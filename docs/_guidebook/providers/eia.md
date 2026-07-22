# Provider dossier: U.S. Energy Information Administration (`eia`)

**Provider:** U.S. Energy Information Administration (EIA) Open Data API v2  ·  **Key:** `eia`  ·  **Homepage:** https://www.eia.gov/opendata/
**Distribution:** `parsimony-eia`  ·  **Namespace(s):** `eia`
**Kind:** public-keyed
**Status:** ✅ verified-live  ·  **Owner:** Andreu  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- **Serves:** U.S. energy statistics — petroleum, natural gas, electricity, coal,
  nuclear, renewables (densified biomass), total energy, CO2 emissions,
  international energy, SEDS, and the STEO/AEO/IEO outlooks.
- **Auth:** required API key as `?api_key=` query param; env var `EIA_API_KEY`
  (present in `ockham/.env`). Free key at https://www.eia.gov/opendata/register.php.
- **Discovery model:** **we build a catalog** (no native search). Archetype **B**
  — walk the v2 route tree to one row per leaf dataset.
- **Total addressable universe:** **232 leaf datasets** (the catalogued tier);
  EIA's own figure for the full series universe is **"2M+ data series"** (the
  facet cartesian product of those datasets), which is uncatalogable but fully
  fetchable. Counted by: walking the route tree (232 leaves, 0 errors) and
  cross-checking the enumerate against an independent walk (exact match).
- **Connectors shipped:** `eia_search`, `eia_fetch`, `eia_fetch_series`,
  `eia_facets`, `enumerate_eia` (5).
- **Completeness verdict:** catalog covers ALL datasets? **YES** (232/232, 0 dups,
  matches independent walk). Connectors cover ALL accessible data? **YES** — every
  series is fetchable by route+facets or by legacy series id; the only "gap" is
  that individual series aren't individually catalogued (by design — 2M+ of them).
- **Known gaps / deliberate exclusions:** XML output (`out=xml`, 300-row cap — we
  use JSON); per-series catalog rows (the 2M-series tier is fetch-only, not
  indexed); a single fetch is capped at 300,000 rows (above it, narrow with
  facets/frequency/date — EIA's own guidance).

**The two findings the live API alone would NOT have surfaced — only reading the
docs did:** (1) the `/v2/seriesid/{id}` legacy path is a whole fetch surface
*outside* the route tree; (2) EIA states the universe is "2M+ series" and blesses
throttled recursive scraping. Both reshaped the design (added `eia_fetch_series`;
confirmed the two-tier catalog/fetch split).

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | API technical documentation | https://www.eia.gov/opendata/documentation.php | api-docs | yes (v2.1.11, Jan 2026) | the authoritative reference |
| 2 | Developer portal / registration | https://www.eia.gov/opendata/register.php | portal | yes | free key, instant |
| 3 | API dashboard / browser | https://www.eia.gov/opendata/browser/ | web | yes | interactive route-tree explorer (mirrors the API) |
| 4 | Copyrights & reuse policy | https://www.eia.gov/about/copyrights_reuse.php | legal | yes | **public domain; redistribution explicitly permitted** |
| 5 | v2.1.0 release PDF | https://www.eia.gov/opendata/documentation/APIv2.1.0.pdf | spec | Nov 2022 | introduced in-return warnings + v1-series-id auto-translation |
| 6 | Hands-on webinar | https://www.eia.gov/opendata/pdf/EIA-APIv2-HandsOn-Webinar-11-Jan-23.pdf | blog | Jan 2023 | worked examples |
| 7 | Community client (R `EIAapi`) | https://ramikrispin.github.io/EIAapi/ | code | yes | confirms pagination/facets idioms |

No OpenAPI/Swagger spec is published. No bulk-download manifest for the v2 API
(the human "Bulk files" download under `/opendata/` is a separate product, not
part of the API and not needed — the route tree is the authoritative enumeration).

### 1.2 The data model (in EIA's terms)

- **Atomic fetchable unit:** an *observation row* (period + facet codes + a
  measure value), retrieved from a **dataset**'s `/data` endpoint. A fully
  specified series = dataset route + one measure + one value per facet dimension.
- **Identification:** the **route path** (`petroleum/pri/spt`) identifies a
  dataset; within it, the `series` facet value (e.g. `RWTC`) or the legacy v1
  series id (`PET.RWTC.D`) identifies a series.
- **Hierarchy:** route tree. A **route node** either lists child `routes` (a
  category) or is a **leaf dataset** carrying `data` (measures), `facets`
  (dimensions), `frequency`, `startPeriod`/`endPeriod`, `defaultFrequency`.
- **Metadata per dataset:** `name`, `description`, the measure ids + units
  (`data{}`), the facet ids + descriptions (`facets[]`), the available
  frequencies, and the date range.
- **Measures universe:** `value` dominates (195/232 datasets); the rest are named
  (`price`, `sales`, `revenue`, `customers`, `heat-content`, `production`,
  `generation`, …). **Every dataset has ≥1 measure** (so `data[0]=` is always
  fillable). **`data[0]=<measure>` is REQUIRED** — without it the `/data`
  endpoint returns metadata rows with no value column.
- **Facets universe:** `duoarea`/`product`/`process`/`series` (165 datasets each,
  the petroleum/gas shape), then `seriesId`, `regionId`, `stateid`, `sectorid`,
  `respondent`, `fueltype`, etc. 2 datasets have 0 facets.
- **Frequencies / period formats** (`dateFormat` is returned per query):
  `YYYY` (annual), `YYYY-MM` (monthly), `YYYY-MM-DD` (daily/weekly),
  `YYYY-"Q"Q` → `2025-Q4` (quarterly), `YYYY-MM-DD"T"HH24` → `2026-06-10T07`
  (hourly), `…TZH` → `2026-06-10T03-04` (local-hourly, trailing TZ band).

### 1.3 Endpoint reference

| Endpoint | Method | Required | Optional | Returns | Pagination | Notes |
|----------|--------|----------|----------|---------|------------|-------|
| `/v2/{route}` | GET | `api_key` | — | child `routes[]` OR leaf dataset metadata | none (inline) | the route-tree node |
| `/v2/{route}/data` | GET | `api_key`, `data[0]=<measure>` | `facets[{id}][]`, `frequency`, `start`, `end`, `sort[N][column/direction]`, `offset`, `length`, `out` | observation rows + `response.total` | `offset`/`length`, **5,000-row JSON cap** | the fetch path |
| `/v2/{route}/facet/{id}` | GET | `api_key` | — | `facets:[{id, name}]` value list | none | facet vocabulary |
| `/v2/seriesid/{SeriesID}` | GET | `api_key` | `start`, `end`, `offset`, `length` | observation rows (same shape as `/data`) | `offset`/`length`, **5,000-row cap** | **legacy v1 series id → out of the route tree** |

- `length` default 5,000 (JSON); a request beyond 5,000 rows is **clamped to
  5,000** and EIA adds a `warning` ("parameter out of range… use offset to
  paginate"). XML (`out=xml`) caps at 300.
- `response.total` is the full count responsive to the query, **independent of
  `offset`/`length`** — the pagination oracle.
- Error body: `{"error": "...", "code": <http status>}`. 400 carries a useful
  message ("The only valid data are 'value'."); 404 for a bad series id
  ("Series ID 'X' is not valid.").

### 1.4 The "full universe" question

- **Authoritative enumeration path:** walk the route tree from `/v2/`, recursing
  on `routes` and terminating at leaf datasets (nodes with `data`/`facets` but no
  `routes`). **Archetype B fan-out.** Route-node child lists are inline and never
  paginated, so the walk is lossless (no bdp-style list-pagination trap).
- **Requires:** recursion + per-node fan-out (~272 node fetches → 232 leaves). No
  pagination at any level.
- **Estimated total:** 232 leaf datasets (catalog tier). EIA's own figure for the
  series tier is **"2M+ data series"** — the facet cartesian product, which the
  catalog deliberately does not enumerate.
- **Outside the enumeration (the treasury-trap):** the **`/v2/seriesid/{id}`
  legacy path** reaches a series by its v1 id without a route — invisible to the
  tree walk. Exposed via `eia_fetch_series`.
- **Gated/unreachable:** nothing material on the free key. XML output and the
  separate human "Bulk files" product are out of scope by choice.

---

## 2. Authentication & access

- **Auth required:** yes. `?api_key=<KEY>` query param (`api_key` is in
  parsimony-core's sensitive-param set → auto-redacted from logs). Header auth is
  not offered by EIA.
- **Obtain:** instant free registration at `/opendata/register.php`.
- **Rate limits:** EIA enforces a short-window throttle; exceeding it
  *temporarily suspends* the key (auto-reactivated after a cooldown) and returns
  an error. Observed live: a 6-wide enumerate fan-out drew 429s → concurrency
  lowered to **4**. The pooled-client retry policy absorbs the rare 429.
- **Human intervention:** none needed — key already in `ockham/.env` as
  `EIA_API_KEY`.
- **Secret handling:** `secrets=("api_key",)` on every keyed verb; env fallback
  `EIA_API_KEY`; fast-fail `UnauthorizedError("eia", env_var="EIA_API_KEY")`
  before any network call.
- **Redistribution:** EIA data is **U.S. federal public domain** — the reuse
  policy explicitly permits redistributing "any of our data, files, databases…
  and other information products". Attribution: "Source: U.S. Energy Information
  Administration". Publishing the id+title catalog is clearly fine.

---

## 3. Transport & quirks

- **Base URL:** `https://api.eia.gov/v2`. JSON only (we ignore XML).
- **`fetch_json` vs chokepoint:** a per-package chokepoint (`eia_get`) is used
  instead of bare `fetch_json` because EIA's **400 carries an actionable message**
  — mapped to `InvalidParameterError` preserving the text (the BdE-412 lesson);
  all other statuses fall through to the canonical kernel mappers.
- **🔴 Pagination is the headline trap.** Every `/data` and `/seriesid` response
  is capped at 5,000 rows. The old single-call `eia_fetch` silently returned the
  first 5,000 of (e.g.) 91,285. Both fetch verbs now read `response.total` and
  page with `offset` to completeness, guarded by a 300,000-row ceiling that
  raises `InvalidParameterError` (echoing EIA's "constrain with facet/start/end")
  rather than truncate or pull millions.
- **Pagination correctness — do NOT add a sort.** Counterintuitively, paging by
  `offset` with **no `sort`** is lossless (EIA's default order is stable), but
  adding `sort[0][column]=period` introduces a **boundary gap+duplicate** when
  many rows share a period (collected == total but one row missed, one repeated —
  masked by the count matching). We page unsorted and **dedup on the natural key**
  (`period` + `series`, or the facet code columns) as insurance.
- **`data[0]=<measure>` required; measure is route-specific.** Default `value`;
  electricity uses `price`/`sales`/`revenue`/`customers`. The selected/detected
  measure is normalized to a `value` column (coercing **only** that column so
  string facet metadata isn't NaN'd — the EIA `duoarea`/`product` lesson).
  Seriesid responses name the measure column inconsistently (`value`+`units` vs
  `sales`+`sales-units`) → detected by the `{col}-units` sibling.
- **Period parsing** covers every frequency incl. quarterly `YYYY-Q#` (expanded to
  the quarter-start month, which pandas can't parse) and local-hourly TZ bands.
- **Anti-bot:** none — plain `httpx` works; no curl_cffi needed.

---

## 4. Catalog plan

- **Strategy:** enumerator + catalog_build, single flat bundle (232 rows — embedding
  cost is negligible at that size).
- **Namespace:** `eia`.
- **Code scheme:** the route path is the KEY (`petroleum/pri/spt`), globally
  unique and hierarchical — no compound code needed.
- **Entity shape:** KEY=`code` (route, ns `eia`), TITLE=`title` (dataset name),
  METADATA=[`description`, `category`, `measures`, `facets`, `frequencies`,
  `default_frequency`, `start`, `end`, `units`]. The `measures`/`facets` columns
  are the **dimension manifest** (the SDMX/BLS pattern): they tell an agent what
  to pass as `measure=`/`facets=` for the next fetch, and are folded into the
  indexed `description` so the vocabulary is BM25-findable.
- **Enumeration:** `enumerate_eia` walks the tree (best-effort per node), emitting
  one row per leaf; `build_eia_catalog(*, api_key=None)` feeds it to a `Catalog`.
- **Index policy:** `discovery_indexes()` → `code` BM25, `title`/`description`
  hybrid (BM25 + vector).
- **Multi-bundle?** No — 232 rows is tiny.
- **Catalog URL:** `hf://parsimony-dev/eia` · env `PARSIMONY_EIA_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Slice of universe |
|-----------|-----------|-------|---------|-------------------|
| `eia_search` | make_local_search_connector | yes | discover datasets | the whole 232-dataset catalog |
| `eia_fetch` | @connector | no | fetch by route+facets, paged | any series in any dataset |
| `eia_fetch_series` | @connector | no | fetch by legacy v1 series id | the out-of-tree seriesid path |
| `eia_facets` | @connector | yes | list a facet's values | narrowing vocabulary |
| `enumerate_eia` | @enumerator | no | catalog feed | all 232 leaf datasets |

- **NOT wrapped (and why):** XML output (JSON suffices); per-series catalog rows
  (2M+ — fetch-only by id); the separate human "Bulk files" product.

## 6. Output schemas

- **Fetch (`EIA_FETCH_OUTPUT`):** KEY=`route` (param_key=route), TITLE=`title`,
  DATA=`period`(datetime)/`value`(numeric); all other EIA columns fold in as DATA.
- **Series (`EIA_SERIES_OUTPUT`):** KEY=`series_id` (param_key=series_id), else as
  above; title prefers the per-row `series-description`.
- **Facets (`EIA_FACETS_OUTPUT`):** KEY=`facet_value`, TITLE=`name`,
  METADATA=`facet`/`route`.
- **Search (`EIA_SEARCH_OUTPUT`):** `code`(KEY)/`title`/`score`. The `code` is the
  route; the agent passes it to `eia_fetch(route=...)`.

## 7. Tests

- `ErrorMappingSuite` wired (`eia_fetch`, route_url `…/petroleum/pri/spt/data`,
  env_key `api_key`, provider `eia`) + a bespoke 400→InvalidParameter assertion.
- Offline (`test_eia_connectors.py`, 33 tests): all 5 verbs, offset-pagination
  assembly, boundary-dedup, the row ceiling, facet param emission, non-value
  measure normalize, seriesid title-from-series-description, route-tree enumerate
  fan-out, parametrized no-key fast-fail (count-guarded), 400/401/429/empty guards.
- Live (`test_integration_eia.py`, 7 tests): both fetch verbs **paging past 5,000
  live**, non-value measure, facet-filter narrowing, facet discovery, bounded
  single-category enumerate (monkeypatched top-route seam), fixture-catalog search
  with ranking discrimination + `assert_no_secret_leak`.
- Recall (`catalog_tests/queries.yaml`): 4 required (3 slash-route `code:` + 1
  lexical title) + 2 optional semantic; `min_required_recall: 1.0`.

## 8. Live verification log

| Date | Check | Expected (docs) | Actual (live) | Verdict | Action |
|------|-------|-----------------|---------------|---------|--------|
| 2026-06-09 | route-tree leaf count | "2M+ series", 14 categories | 14 top routes, 26 parents, **232 leaves, 0 errors** | ✅ | catalog the 232 datasets |
| 2026-06-09 | `data[0]=` required | yes | without it: 200 + rows but no `value` column | ✅ | measure param, default `value` |
| 2026-06-09 | 5,000-row cap | "will not return more than 5,000 rows" | `length=9000` → exactly 5,000; `total=91285` | ✅ refuted single-call | **paginate by offset** |
| 2026-06-09 | offset paging lossless | docs recommend sorting | **unsorted lossless** (7847/7847 uniq); **+sort → gap+dup** | ⚠️ docs misleading | page unsorted + dedup natural key |
| 2026-06-09 | route-node child lists paginate? | unstated | no — `routes` inline, no `total`/`offset` | ✅ | walk is complete |
| 2026-06-09 | facet filter param | `facets[{id}][]` | accepted; `facets[series][]=RWTC` → total 91285→7560 | ✅ | `facets={}` param |
| 2026-06-09 | invalid measure | 400 + message | `400 {"error":"...valid data are 'value'","code":400}` | ✅ | InvalidParameterError, keep msg |
| 2026-06-09 | **`/v2/seriesid/{id}`** | "maintained until ≥ Jan 2023" | **live**: `PET.RWTC.D`→total 10176; bogus→404 | ✅ | **add `eia_fetch_series`** |
| 2026-06-09 | seriesid measure col | — | `value`+`units` (petroleum) vs `sales`+`sales-units` (electricity) | ✅ | detect by `-units` sibling |
| 2026-06-09 | fetch pagination live | — | `petroleum/pri/spt` daily → **91,285 rows** (was 5,000); WTI seriesid → 10,176 | ✅ | bug fixed, verified |
| 2026-06-09 | enumerate vs independent walk | match | **232 == 232, 0 in-walk-not-enum, 0 in-enum-not-walk** | ✅ | Q1 proven |
| 2026-06-09 | catalog build + recall | required_recall 1.0 | schema_ok, 232 entries, **required_recall 1.00** (slash-route `code:` works) | ✅ | Q1 recall proven |
| 2026-06-09 | row ceiling | — | electricity hourly `total=18,675,533` → InvalidParameterError | ✅ | narrow-guidance, no runaway |
| 2026-06-09 | redistribution | public domain | reuse policy permits redistributing data products + attribution | ✅ | publish OK |

**Completeness sign-off:** the catalog contains ALL **232 leaf datasets**
(verified by enumerate count == independent route-tree walk, 0 dups; recall gate
1.00) and the connectors expose every accessible data class — every series is
fetchable by route+facets (`eia_fetch`) or by legacy id (`eia_fetch_series`),
with `eia_facets` supplying the narrowing vocabulary. The ~2M individual series
are deliberately not catalogued (fetch-only by id). Signed: Andreu, 2026-06-09.

## 9. Open questions / follow-ups

- [ ] **Publish the catalog snapshot** to `hf://parsimony-dev/eia` (built locally,
  232 entries, validated; deferred maintainer step needing `HF_TOKEN`) + add `eia`
  to the catalog-validate CI matrix.
- [ ] If an agent frequently needs a *specific* high-traffic dataset's series list,
  consider a lazy per-dataset series tier (BLS-style) — not built (the 2M universe
  + `eia_facets` narrowing make it unnecessary for now).
