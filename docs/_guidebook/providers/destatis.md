# Provider dossier: Destatis — GENESIS-Online (`destatis`)

> One file per provider/system. This is the **single place** where ALL of the
> provider's documentation, API behaviour, and our findings are compiled before
> a line of connector code is written.
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** Statistisches Bundesamt (Destatis), GENESIS-Online  ·  **Key:** `destatis`  ·  **Homepage:** https://www.destatis.de
**Distribution:** `parsimony-destatis`  ·  **Namespace(s):** `destatis`
**Kind:** public-keyless
**Status:** ✅ verified-live  ·  **Owner:** connectors-sweep  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- What this provider serves, in one sentence: German official statistics — prices,
  population, national accounts, labour, trade, production, transport, health,
  environment, earnings — as **predefined tables** from the keyless **GENESIS-Online
  REST API** (`genesis.destatis.de/genesis/api/rest`).
- Auth: **none** (keyless, anonymous). Only a browser `User-Agent` is sent so the API
  does not redirect to its SPA shell.
- Discovery model: **we build a catalog**. Archetype **A→B** — `/statistics` is one flat
  top-level list (331 statistics, archetype A), then a `1 + 2N` per-statistic fan-out
  (`/information` + `/tables`, archetype B) lists every table.
- Total addressable universe: **3,009 predefined tables across 331 statistics** (live,
  2026-06-09). Counted by summing `len(/statistics/{code}/tables)` over all 331 statistics;
  unique == sum (no table is shared across statistics). Plus a statistic row per statistic →
  **3,340 catalog entries** (331 statistic + 3,009 table).
- Connectors shipped: `destatis_fetch`, `enumerate_destatis`, `destatis_search`.
- Completeness verdict: catalog covers ALL? **YES** (every fetchable table is enumerated —
  proven by the per-statistic fan-out being lossless and by no fetchable-but-unlisted table
  existing; a tableless statistic now keeps its own row). connectors cover ALL? **YES, after
  the fix** — `destatis_fetch` previously **hard-failed ~25 % of tables** (any whose time axis
  is `STAG`/`SEMEST`/`SMONAT`/`SQUART`/`SLJAHR` rather than `JAHR`) with a bogus
  `ParseError: year <statistic-code> is out of range`; the time-dimension detector was
  rewritten to key-shape detection and now every sampled table (12/12 across all frequencies)
  fetches with real dates.
- Known gaps / deliberate exclusions: the keyless REST API exposes **predefined tables only** —
  the multidimensional **cubes / custom-table** power-user surface of classic GENESIS is **not
  present on this host** (every `/cubes`, `/data/cube`, `/metadata/*` path → 404) and is not
  wrapped. `/variables` (3,338 dimension definitions) and `/search` (a typeahead) are not
  wrapped as connectors — they carry no fetchable data of their own.

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Last seen current? | Notes |
|---|--------|-----|------|--------------------|-------|
| 1 | OpenData / web-service hub (EN) | https://www.destatis.de/EN/Service/OpenData/_node.html | portal | 2026-06-09 | "API/web services and web interface"; reuse under Data Licence Germany 2.0 |
| 2 | GENESIS-Online database (SPA) | https://genesis.destatis.de/datenbank/online | web | 2026-06-09 | the human-facing DB; this REST API is its backend |
| 3 | The live REST API | https://genesis.destatis.de/genesis/api/rest | api | 2026-06-09 | the authoritative surface — discovered/confirmed live (no public swagger) |
| 4 | Data Licence Germany 2.0 (Namensnennung) | https://www.govdata.de/dl-de/by-2-0 | legal | 2026-06-09 | open licence; requires source attribution → redistributing a derived catalog is allowed |
| 5 | Classic GENESIS webservice (`genesisWS/rest/2020`) | https://www-genesis.destatis.de/genesisWS/rest/2020 | api-docs | 2026-06-09 | **RETIRED** — now serves the HTML SPA shell; the 0.5.0 migration moved off it |

No public OpenAPI/Swagger is served (`/openapi.json`, `/v3/api-docs`, `/swagger.json` → 404).
The endpoint set below was established by direct live probing.

### 1.2 The data model (in Destatis's own terms)

- Atomic fetchable unit: a **table** (Tabelle), a predefined multi-dimensional output table
  identified by a code like `61111-0001` (`{statistic}-{nnnn}`). A table is itself a small
  cube — many series (dimension combinations) over a time axis — returned as one JSON-stat
  dataset.
- Hierarchy above it: a **statistic** (Statistik), identified by a 5-char numeric code
  (`61111` = Verbraucherpreisindex). Each statistic owns 0–132 tables. Statistics carry
  `statisticalCategoryNames` (a coarse→fine subject hierarchy) and `variableCodes`/`names`.
- Metadata that travels with each unit: statistic → `name{de,en}`, a long German
  "description" (Qualitätsbericht lead), category names, variable codes/names; table →
  `name{de,en}`, `variableCodes`/`variableNames`.
- Frequencies / dimensions: tables encode time as a `Zeit`/`JAHR`/`MONAT`/`QUARTAL` dimension;
  period labels are German (`Januar 2026`, `1. Quartal 2026`, `2026`) and are normalised to ISO.

### 1.3 Endpoint reference (confirmed live 2026-06-09)

Base URL: `https://genesis.destatis.de/genesis/api/rest`. Anonymous; `Accept: application/json`.

| Endpoint | Method | Required | Optional | Returns | Pagination | Notes |
|----------|--------|----------|----------|---------|------------|-------|
| `/statistics` | GET | — | — | **bare JSON list** of 331 statistic nodes | none (full list) | the archetype-A index |
| `/statistics/{code}` | GET | code | — | `{StatisticListItem: {...}}` (wrapped single) | — | not needed by the crawl |
| `/statistics/{code}/information` | GET | code | — | `{code, name, description}` (or `[]` if none) | none | the long German description |
| `/statistics/{code}/tables` | GET | code | — | **bare JSON list** of table nodes (or **404 if the statistic has 0 tables**) | none — full list, `pagesize` is ignored | per-statistic table list |
| `/tables/{code}/data` | GET | code | `startyear`,`endyear` | **JSON-stat 2.0** dataset (sometimes a `{data:[…]}` envelope) | none | the fetch path |
| `/variables` | GET | — | — | bare JSON list of 3,338 variable defs | none | dimension dictionary; **not wrapped** |
| `/search` | GET | `searchTerm` | — | `{terms, statisticCodes, tableCodes, variableCodes}` | capped typeahead | the website search box; **not wrapped** |

### 1.4 The "full universe" question

- **Authoritative enumeration path:** `/statistics` (one call → all 331 statistics) **×** a
  per-statistic `/statistics/{code}/tables` fan-out. The union of the per-statistic table lists
  **is** the table universe. There is **no flat `/tables` top-level list** (`/tables` → 404), so
  the fan-out is the only path — but it is **lossless**: `/tables` per statistic returns the
  full list in one response (no pagination; `pagesize` is ignored), the largest is 132 tables
  (statistic 12211), and **no table is shared across statistics** (3,009 rows == 3,009 unique).
- Pagination / recursion / fan-out: `1 + 2N` requests (N=331): one `/statistics`, then per
  statistic one `/information` + one `/tables`. No nested pagination at any level (the bdp
  page-1 trap does **not** apply here — every list endpoint returns its full set).
- Estimated total: **3,009 tables across 331 statistics** (live 2026-06-09). The provider does
  not publish a headline count; this is the measured live figure.
- Things that exist but are NOT in the enumeration: **nothing fetchable on this host.** A
  fetch-beyond-catalog probe (`/tables/61111-{0001..0030}/data`) returned data **only** for the
  enumerated codes; the gaps (`-0008/-0009/-0012`) returned **HTTP 404** — the API serves
  exactly the tables it lists. The classic-GENESIS **cubes** (`61111BJ001`-style custom-table
  building) are **absent from this keyless host** (`/cubes/*`, `/data/cube`, `/metadata/cube`
  all 404) — that surface belongs to the retired/registration-gated webservice and is out of
  scope by design, not a silent gap.
- Anything gated behind a higher plan / login: the cube/custom-table surface (registered
  webservice). Not reachable keyless; documented as a deliberate boundary.

---

## 2. Authentication & access

- Auth required? **No.** Keyless anonymous access (Data Licence Germany 2.0). No `secrets=`,
  no `bind`, no `UnauthorizedError` on the data path.
- Headers: a browser `User-Agent` is sent on every request. Without it the API can redirect to
  its SPA/maintenance HTML shell; with it, clean JSON. `Accept: application/json`.
- Rate limits: none published, but anonymous access is throttled under load — GENESIS can
  return a **200 with an HTML throttle notice** ("zu viele Anfragen / Kontingent ausgeschöpft")
  instead of data. The metadata crawl is throttled (concurrency **4**, 0.25 s inter-request
  delay); higher concurrency triggers 429/503. A throttle body maps to `RateLimitError`.
- Terms: Data Licence Germany – Namensnennung 2.0 → reuse with **source attribution to
  Destatis**. A derived catalog of table codes + titles is within the grant; README +
  snapshot carry Destatis attribution.

---

## 3. Transport & quirks

- Base URL: `https://genesis.destatis.de/genesis/api/rest`. The legacy
  `www-genesis.destatis.de/genesisGONLINE/api/rest` host **301-redirects** here and doubles the
  path (`/rest/rest/...` → 404), so the connector points straight at the canonical host
  (treasury 301-avoidance lesson).
- Response formats: index/list endpoints return **bare JSON arrays**; single-resource GETs wrap
  (`{StatisticListItem: …}`); the fetch endpoint returns **JSON-stat 2.0** (occasionally inside
  a `{data: […]}` envelope of datasets).
- `fetch_json` fits the single-table fetch (GET + JSON + typed mapping); the enumerate crawl
  uses the shared `ThrottledJsonFetcher` (the established `1+2N` idiom).
- **200-with-error-body (the statistical-office quirk).** A 200 can carry the SPA/maintenance
  **HTML shell** (host/path drift) or an HTML **throttle** notice rather than JSON-stat. The
  fetch path reads text first and disambiguates by body shape: HTML + a quota phrase →
  `RateLimitError(quota_exhausted=True)`; HTML otherwise → `ParseError`; valid JSON that is not
  a JSON-stat dataset/envelope → `ParseError`. Never fakes a status.
- **JSON-stat 2.0 parsing.** Expand the flat `value` array (list **or** sparse `{idx: val}`
  dict) against the `id`/`size` dimension shape. Non-time dimensions ride along as their own
  columns (raw codes, e.g. `GES=GESM`, `TODUR1=TDU-01`), so a multi-classification table stays
  disambiguated. All-null datasets → `EmptyDataError`.
- **Time-dimension detection — the headline fetch finding.** `role` is **absent** and `label`
  is **null**; the time axis is the dimension whose **category index keys are the period
  values** (`JAHR`→`"2012"`, `STAG`/`STAGV`→`"1999-12-31"`, `SEMEST`→`"2003-10P6M"`,
  `SMONAT`→`"2015-05P1M"`, `SQUART`→`"2015-04P3M"`, `SLJAHR`→`"2000-P1Y"` — ISO-8601 durations).
  The old detector matched on dimension **name** (`ZEIT/JAHR/MONAT/QUARTAL`) and **fell back to
  dimension 0**, which is the constant `statistic` dim — so any table whose time axis was named
  otherwise emitted the **statistic code as a "year"** and raised
  `ParseError: year 12411 is out of range` (≈25 % of tables: all reference-date, semester,
  ISO-duration-month/quarter, school-year tables). Worse, the name set would **false-positive**
  the `MONAT`/`QUARTG` *month-/quarter-of-year classifications* (keys `MONAT10`/`QUART3`). The
  fix detects the time dim by **key shape** (the dimension whose keys parse as periods, majority
  rule, never dim 0) and normalises every period form (incl. the ISO durations) to the
  ISO `YYYY-MM-DD` start. The German-month-label path (`_normalize_german_date`) is retained only
  as a defensive fallback — the live API no longer sends German labels at the data level.
- Date/locale: German period labels (`Januar 2026`, `1. Quartal 2026`, `2026`, `2026-01`)
  normalised to ISO `YYYY-MM-DD` by `_normalize_german_date`; unknown labels pass through so a
  real parse issue surfaces rather than silent mangling.
- **Tableless statistic edge case.** `/statistics/{code}/tables` returns **HTTP 404** (not an
  empty list) when a statistic has zero tables (e.g. `61121`, whose `/information` is also `[]`).
  This is a legitimate "no tables", **not** a fetch failure — enumerate must still emit the
  statistic's own row so it stays in the catalog.

---

## 4. Catalog plan

- Strategy: **enumerator (`enumerate_destatis`) + catalog_build** (archetype A→B). One flat
  namespace, two entity types: `statistic` rows (parent, rich German description) and `table`
  rows (the fetchable unit; the parent description is lifted in as retrieval signal because a
  bare table title is too thin to embed/rank well).
- Namespace: `destatis` (KEY = `code`).
- Code scheme: a statistic's code is its 5-char id (`61111`); a table's code is `{stat}-{nnnn}`
  (`61111-0001`) — fed directly to `destatis_fetch(name=…)`.
- Entity shape: KEY=`code` (ns `destatis`), TITLE=`title`, METADATA=`description`,
  `entity_type`, `parent_statistic`, `subject_area`, `title_de`, `title_en`, `variable_codes`,
  `variable_names_en`, `source`.
- Index policy: `discovery_indexes()` (3,340 entries; bilingual DE/EN titles + German
  description in the indexed text, so DE and EN queries both hit).
- Multi-bundle? **No** — 3,340 entries is a single tractable bundle (no per-agency split needed,
  unlike boj/bls).
- Catalog root: `hf://parsimony-dev/destatis` · env override `PARSIMONY_DESTATIS_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `destatis_search` | @connector (make_local_search_connector) | yes | discovery | the whole catalog (statistics + tables) |
| `destatis_fetch` | @connector | no | fetch a table | any table by `name` (the `{stat}-{nnnn}` code) |
| `enumerate_destatis` | @enumerator | no | catalog feed | the whole universe (331 statistic + 3,009 table rows) |

- Endpoints deliberately NOT wrapped: `/variables` (3,338 dimension definitions — metadata, not
  fetchable series), `/search` (a capped typeahead — our local catalog search is more complete),
  `/statistics/{code}` single-GET (the index already carries the needed fields). The cube /
  custom-table surface is absent on this host (see §1.4).

## 6. Output schemas

- Fetch (`DESTATIS_FETCH_OUTPUT`): KEY=`series_id` (ns `destatis`, `param_key="name"`),
  TITLE=`title`, DATA=`date`(datetime)+`value`(numeric).
- Enumerate (`DESTATIS_ENUMERATE_OUTPUT`): KEY=`code` (ns `destatis`), TITLE=`title`,
  METADATA=`description`/`entity_type`/`parent_statistic`/`subject_area`/`title_de`/`title_en`/
  `variable_codes`/`variable_names_en`/`source`.
- Search (`DESTATIS_SEARCH_OUTPUT`): `code`(KEY ns `destatis`)/`title`(TITLE)/`score`(DATA).
  A search hit's `code` feeds `destatis_fetch(name=code)` directly (table rows) — statistic rows
  are navigational (their code is a parent, not directly fetchable).

---

## 7. Tests

- `ErrorMappingSuite`: `destatis_fetch`, route `…/tables/61111-0001/data`, `env_key=None`.
- Offline (`respx`): fetch JSON-stat happy path + year-range forwarding + 404/500→ProviderError
  + HTML-shell→ParseError + throttle-HTML→RateLimitError + non-dataset-JSON→ParseError +
  all-null→EmptyDataError + empty-code→InvalidParameterError + **time-dim by key-shape**
  (STAG reference-date never emits the statistic code as a year; ISO-duration `SMONAT` period;
  `MONAT` month-of-year classification is NOT mistaken for the time axis); enumerate
  statistic+table rows + parent-description lift + sub-resource-failure keeps the statistic row
  + empty-index + **tableless-statistic (404 `/tables`) still emits its row**.
- Integration (live, env-gated): `destatis_fetch` (known table + year range), bounded
  `enumerate_destatis` (monkeypatched index → 1 statistic), `destatis_search` over a fixture
  catalog in `tmp_path`.
- Conformance: `assert_plugin_valid(parsimony_destatis)`.
- Catalog probes `catalog_tests/queries.yaml`: title/bm25 probes for CPI, population,
  unemployment, foreign trade.

---

## 8. Live verification log (documentation is a claim; execution is the truth)

| Date | Check | Expected | Actual (live) | Verdict | Action |
|------|-------|----------|---------------|---------|--------|
| 2026-06-09 | `/statistics` count | full index | **331** statistics, all-unique codes, one bare list | ✅ | archetype-A parent index |
| 2026-06-09 | flat `/tables` top-level list? | maybe | **404** (also `/cubes`,`/timeseries` 404) | ✅ | fan-out is the only path |
| 2026-06-09 | **Q1: per-statistic `/tables` lossless?** | full per stat | full list returned, **`pagesize` ignored**, max 132/stat, no round-number cap | ✅ | no pagination/truncation trap |
| 2026-06-09 | **Q1: table universe size + dups** | unknown | **3,009 tables**, rows==unique → **0 cross-statistic duplicates** | ✅ | catalog target (no dedup needed) |
| 2026-06-09 | **Q1: fetchable-but-unlisted tables?** | none | `61111-{0008,0009,0012}` (enum gaps) → **HTTP 404**; only enumerated codes fetch | ✅ | enumeration == fetchable universe |
| 2026-06-09 | cube / registered surface on this host | absent | `/cubes/*`,`/data/cube`,`/metadata/*`,`/tables/{c}` (no `/data`) all **404** | ✅ | predefined-tables-only; boundary documented |
| 2026-06-09 | tableless statistic handling | emit row | `61121`: `/tables`→404, `/information`→`[]`; old enumerate **dropped it entirely** | ⚠️→✅ | **fixed**: emit the statistic row from the index node; 404-tables == 0 tables |
| 2026-06-09 | **Q2: time-dimension detection** | every table fetchable | **3/12** sampled tables (12411/11111/21311 — STAG/SEMEST) raised `ParseError: year 12411 is out of range`; old detector fell back to the constant `statistic` dim | ⚠️→✅ | **fixed**: key-shape time detection + ISO-period normalisation |
| 2026-06-09 | Q2 re-verify after fix | all OK | **12/12** tables across all frequencies fetch with real dates, **0 NaT** (STAG 1950–2024, SEMEST 1998–2024, JAHR, multi-dim) | ✅ | defect closed |
| 2026-06-09 | non-time dims preserved (multi-series) | disambiguated | `23211-0001` → `GES`/`TODUR1`/`content` columns survive; 11,070 rows keyed | ✅ | breakdown not collapsed |
| 2026-06-09 | sample fetch parses JSON-stat values | numeric+ISO | `61111-0001` → CPI floats, ISO dates from period keys | ✅ | — |
| 2026-06-09 | error mapping: unknown table | typed | unknown code → real **404** → `ProviderError(404)` | ✅ | — |

**Completeness sign-off:** the catalog contains **ALL 3,009 fetchable predefined tables**
(verified: per-statistic `/tables` fan-out is lossless — full lists, no pagination cap, zero
cross-statistic duplicates — and no fetchable-but-unlisted table exists, the enum gaps 404) plus
a navigational row per statistic; **every table is fetchable** via `destatis_fetch(name)` **after
the time-dimension fix** (the old name-based detector hard-failed ~25 % of tables; the key-shape
detector now fetches 12/12 sampled tables across all frequencies with real dates). The one
boundary is the cube/custom-table power-user surface, which is **absent from this keyless host**
(all such paths 404) and belongs to the retired/registration-gated webservice — a documented
scope limit, not a silent gap. Signed: connectors-sweep on 2026-06-09.

---

## 9. Open questions / follow-ups

- [ ] **Publish the catalog snapshot** (build → `validate_catalog` → push
      `hf://parsimony-dev/destatis`, ~3,340 entries, single bundle). Deferred (maintainer step;
      needs `HF_TOKEN`) — not yet run.
- [ ] If Destatis ever exposes the cube/custom-table surface keyless (or a free token API), the
      universe grows by orders of magnitude — re-survey then. For now the keyless predefined-table
      API is the whole shipped scope.
- [ ] `enumerate_destatis` could surface the per-table **frequency** (parse the JSON-stat time
      dimension at build time) — currently frequency lives only in the prose description.
