# Provider dossier: Bank of Japan (`boj`)

> One file per provider/system. This is the **single place** where ALL of the
> provider's documentation, API behaviour, and our findings are compiled before
> a line of connector code is written.
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** Bank of Japan (日本銀行)  ·  **Key:** `boj`  ·  **Homepage:** https://www.boj.or.jp
**Distribution:** `parsimony-boj`  ·  **Namespace(s):** `boj` (fetch/enumerate), `boj_databases` + `boj_series_<db>` (catalog bundles)
**Kind:** public-keyless
**Status:** ✅ verified-live  ·  **Owner:** connectors-sweep  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- What this provider serves, in one sentence: Japanese macro + financial time series
  (interest rates, FX, money & deposits, flow of funds, TANKAN, prices/CGPI/SPPI,
  balance of payments, BIS-related statistics) via the **BOJ Time-Series Data Search**
  API (`stat-search.boj.or.jp/api/v1`).
- Auth: **none** (keyless public JSON/CSV API). `load(*, catalog_root=...)` binds only the
  catalog root.
- Discovery model: **we build a catalog** (no native keyword search). Archetype **C + B** —
  a frozen 50-entry **DB registry** (the API exposes no way to list databases) × a live
  per-DB `getMetadata` fan-out that lists every series in each database.
- Total addressable universe: **326,466 series across 50 databases** (live, 2026-06-09).
  Measured by summing `len(getMetadata(db).RESULTSET series rows)` over all 50 DBs.
- Connectors shipped: `boj_fetch`, `enumerate_boj`, `boj_databases_search`, `boj_series_search`.
- Completeness verdict: catalog covers ALL? **YES, two-tier** (Q1 — the databases tier is
  complete at 50/50; each per-DB series tier is complete because `getMetadata` is uncapped;
  every series is fetchable by id). connectors cover ALL? **YES** (Q2 — every series reachable
  via `boj_fetch(db, code)`, all frequencies; the `getDataCode` 250-code / 60,000-point limit
  is now handled by `NEXTPOSITION` pagination).
- Known gaps / deliberate exclusions: `getDataLayer` (tree-addressed retrieval of the *same*
  data `getDataCode` reaches by code) and CSV output are not wrapped — no unique data. The
  giant DBs (CO/TANKAN 166k, FF 34k, BIS 34k, PR01 31k, PR03 27k, BP01 18k) are catalogued
  per-DB lazily (BM25-only above 1,000 unique titles) — discovery is bounded, access is total.

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Last seen current? | Notes |
|---|--------|-----|------|--------------------|-------|
| 1 | API User Manual (EN) | https://www.stat-search.boj.or.jp/info/api_manual_en.pdf | api-docs | 2026-06-09 | 27 pp, dated **2026-02-18**. The authoritative spec: 3 endpoints, the DB-name table, the limit values |
| 2 | API User Manual (JP) | https://www.stat-search.boj.or.jp/info/api_manual.pdf | api-docs | 2026-06-09 | Japanese original |
| 3 | API Request URL Tool | https://www.stat-search.boj.or.jp/info/api_tool.xlsx | bulk | 2026-06-09 | XLSX with a **`DB_Name` sheet** = the machine-readable 50-DB list (code, category, name, EN+JP). The harvester source |
| 4 | Notice Regarding Use of the API | https://www.stat-search.boj.or.jp/info/api_notice_en.pdf | legal | 2026-06-09 | terms / "avoid excessive access" |
| 5 | Release schedule | https://www.boj.or.jp/en/statistics/outline/index.htm | info | 2026-06-09 | data available ~08:50 JST; metadata refreshed daily |

### 1.2 The data model (in BoJ's own terms)

- Atomic fetchable unit: a **series**, identified by an opaque alphanumeric **series code**
  (e.g. `FXERD01`, `MADR1Z@D`, `TK99F0000601GCQ00000`). Codes are **scoped to a database** —
  the search screen shows a "time-series data code" `IR01'MADR1Z@D` (DB-prefixed), but the API
  takes the bare `code` plus a separate `db` parameter; passing the prefixed form errors.
- Hierarchy above it: a **database (DB)** — one of 50 named statistics databases
  (`FM08` = Foreign Exchange Rates, `CO` = TANKAN, `FF` = Flow of Funds). Each DB groups its
  series into a 5-level **Layer** tree; section headers in the metadata carry the layer titles.
- Metadata per series (`getMetadata`): `SERIES_CODE`, `NAME_OF_TIME_SERIES`, `FREQUENCY`,
  `UNIT`, `CATEGORY`, `START/END_OF_THE_TIME_SERIES`, `LAST_UPDATE`, `NOTES`, `LAYER1..5`.
- Frequencies: Daily, Weekly (W0–W6), Monthly, Quarterly, Semi-annual (calendar/fiscal half),
  Annual (calendar/fiscal year). A single `getDataCode` request may only mix **one** frequency.

### 1.3 Endpoint reference (manual §I–III, confirmed live)

Base URL: `https://www.stat-search.boj.or.jp/api/v1`. Output: `format=json|csv` (we use JSON);
`lang=en|jp`. httpx auto-negotiates gzip (the manual recommends `Accept-Encoding: gzip`).

| Endpoint | Method | Required | Optional | Returns | Notes |
|----------|--------|----------|----------|---------|-------|
| `/getMetadata` | GET | `db` | `format`,`lang` | `{RESULTSET:[…], STATUS, MESSAGEID, MESSAGE, DB, DATE}` | series + section-header rows for one DB; **uncapped, no NEXTPOSITION** |
| `/getDataCode` | GET | `db`,`code` | `startDate`,`endDate`,`startPosition`,`format`,`lang` | `{RESULTSET:[{SERIES_CODE, VALUES:{SURVEY_DATES, VALUES}}], NEXTPOSITION?, …}` | the fetch path; **250 codes & 60,000 points per request, paginated via `NEXTPOSITION`** |
| `/getDataLayer` | GET | `db`,`layer`,`frequency` | `startDate`,`endDate`,`startPosition` | same data shape, addressed by Layer tree | **not wrapped** — same data `getDataCode` reaches by code; 1,250-code hard cap |

Date params follow the series frequency: `YYYY` (annual/fiscal), `YYYYHH` (half), `YYYYQQ`
(quarter), `YYYYMM` (monthly/weekly/daily). Suppressed/missing observations come back as
`null` (still counted toward the 60,000-point limit).

### 1.4 The "full universe" question

- **Authoritative enumeration path:** there is **no single live full-index** endpoint. The
  universe is `(the 50-DB registry) × (live getMetadata per DB)` — archetype **C + B**:
  - **The DB list is frozen** because the API exposes *no* method to enumerate databases
    (`getMetadata` with no `db` → HTTP 400; `getStatsList`/`getDbList`/etc. → 404). The 50
    codes are transcribed from the manual §II.3.(2) **and cross-validated against the
    machine-readable `api_tool.xlsx` `DB_Name` sheet** — both agree exactly, zero diff
    (`scripts/harvest_databases.py` regenerates the registry from the XLSX).
  - **Each DB's series list is live and complete.** `getMetadata(db)` returns every series for
    that DB in a single response with **no NEXTPOSITION and no row cap** — verified across all
    50 DBs, including `CO` (TANKAN) which returns **166,513 series in one ~99 MB response**.
- Pagination / recursion: `getMetadata` is one call per DB (50 calls total). `getDataCode`
  paginates on `NEXTPOSITION` (the *fetch* path, not enumeration).
- Estimated total: **326,466 series** across 50 DBs (live 2026-06-09). Biggest DBs:
  CO 166,513 · FF 33,859 · BIS 33,670 · PR01 31,254 · PR03 27,154 · BP01 17,989 · LA01 3,599.
- Things that exist but are NOT enumerable: **the DB list itself** (frozen, hence the
  cross-validated registry + harvester). Nothing else — `getMetadata` is the complete
  per-DB index, and the API "provides access to all available data" (manual §I.3).
- Anything gated behind a higher plan / login: none — fully open and keyless.

---

## 2. Authentication & access

- Auth required? **No.** Keyless public API. No `secrets=`, no `bind(api_key=…)`, no
  `UnauthorizedError` on the data path. `load(*, catalog_root=None)` binds only the catalog
  root for the two search connectors.
- Headers: the connector sends a browser `User-Agent` on every request. BoJ's
  `stat-search.boj.or.jp` sits behind Akamai; from the dev/CI network probed (2026-06-09) it
  returned HTTP 200 with both the default httpx UA and the browser UA — no 403 observed — but
  the browser UA + a concurrency cap of 2 + a small inter-request delay on the metadata crawl
  is kept as defence (manual §I.2 cautions "excessive access frequency may result in a
  restriction of access").
- Rate limits: none published; "please avoid excessive requests." The metadata crawl is
  throttled (concurrency 2, 0.5 s delay, retries on 403/429/5xx).
- Terms (api_notice): public reuse with source attribution to the Bank of Japan. A derived
  catalog of series ids + titles is within the grant; README + snapshot carry BoJ attribution.

---

## 3. Transport & quirks

- Base URL: `https://www.stat-search.boj.or.jp/api/v1`. `fetch_json` fits (GET + JSON +
  `check_status` + typed-error mapping). gzip is negotiated transparently by httpx.
- **`getDataCode` 250-code / 60,000-point cap with `NEXTPOSITION` pagination — the headline
  finding.** A request whose `(series × periods)` exceeds **60,000 data points** returns
  **HTTP 200 + `STATUS:200, MESSAGE:"Successfully completed"`** but with only the first *K*
  series, and a top-level **`NEXTPOSITION`** integer naming the 1-based series position to
  resume from. The old `boj_fetch` never read `NEXTPOSITION`, so any multi-series request over
  the point cap **silently dropped the tail** (measured: 22 daily FX series → only 5 returned,
  NEXTPOSITION=6, 17 series lost, no error). The fix paginates: re-request with
  `startPosition=NEXTPOSITION` until it is absent, accumulating series across pages (a
  non-advancement guard + page cap prevents pathological loops). Truncation is at **series
  boundaries** (position-based), so resume is lossless — verified 22/22 series reconstructed
  across 3 pages.
- **No single series exceeds the point cap.** The longest series is `IR01 / MADR1Z@D` (the
  Basic Discount Rate, daily from 1882) at **52,470 points** — under 60,000 — so a single-series
  fetch is always complete in one page; only multi-series requests trip pagination.
- **`getMetadata` is uncapped.** Top-level keys are only `{RESULTSET, STATUS, MESSAGEID,
  MESSAGE, DB, DATE}` — no NEXTPOSITION on metadata, even for the 166,513-series CO response.
  This is what makes the per-DB series catalog complete.
- Date/number formats: survey dates are compact frequency-dependent integers (`YYYYMMDD`
  daily, `YYYYMM` monthly, `YYYYQQ` quarter, `YYYY` annual) — parsed by `_parse_boj_date`;
  unknown widths pass through unchanged so a real parse error surfaces rather than silent
  mangling. Values are decimal strings; `null` for suppressed/missing.
- Error/status (manual §III.5): `200 / M181030I` = "completed with no applicable data"
  (→ `EmptyDataError`), `400` = parameter errors incl. `M181005E` invalid DB / `M181013E`
  nonexistent code (→ `ProviderError(400)`), `500 / M181090S` and `503 / M181091S` = server
  (→ `ProviderError`). Mapped by `fetch_json`'s canonical table.

---

## 4. Catalog plan

- Strategy: **enumerator (`enumerate_boj`) + multi-bundle catalog_build** (archetype C+B). One
  `enumerate_boj` fans `getMetadata` across the 50-DB registry and emits one row per series
  **plus** one synthetic `db:<code>` row per DB; `catalog_build.split_enumerated_entries`
  partitions those flat rows into a **databases** bundle + one **per-DB series** bundle.
- Namespaces: `boj_databases` (the 50 DB rows), `boj_series_<db>` (one per database). The
  fetch/enumerate KEY namespace is `boj`.
- Two-step search (sdmx/bls shape): `boj_databases_search` (step 1: find the DB) →
  `boj_series_search(db=…)` (step 2: find the series, lazy-built + LRU-cached per DB) →
  `boj_fetch(db, code)`.
- Why two-tier (the scale rationale): the universe is 326k series; CO alone is 166k. A single
  flat catalog would be unwieldy and the embedder would choke. Per-DB bundles keep each
  namespace tractable; above 1,000 unique titles the adaptive index is BM25-only (scales to
  166k rows without embedding). This mirrors the **bls** survey/series two-tier.
- Code scheme: a series row's KEY is its bare code (`FXERD01`); a DB row's KEY is `db:<code>`
  (`db:FM08`), rewritten to a bare `FM08` in the `boj_databases` bundle. Series→fetch routing
  needs both `code` and `db`, so `boj_series_search` returns `db` alongside `code`.
- Registry liability mitigations (archetype C discipline, §7.2): the 50-DB list is
  cross-validated against the XLSX, a `harvest_databases.py` reproduction script is committed,
  and a floor/shape test pins `len == 50` + asserts the historical phantom `BP02` is absent.
- Catalog root: `hf://parsimony-dev/boj` (multi-bundle) · env override `PARSIMONY_BOJ_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `boj_databases_search` | @connector (search) | yes | discovery step 1 | the 50 databases |
| `boj_series_search` | @connector (search) | yes | discovery step 2 | every series in one DB (lazy per-DB bundle) |
| `boj_fetch` | @connector | no | fetch observations | any series by `(db, code)`, ≤250 codes, NEXTPOSITION-paginated |
| `enumerate_boj` | @enumerator | no | catalog feed | the whole universe (326k series + 50 DB rows) |

- Endpoints deliberately NOT wrapped: `/getDataLayer` (retrieves the *same* time-series data
  `getDataCode` reaches by code, just addressed by Layer-tree position — no unique data); CSV
  output (JSON suffices).

## 6. Output schemas

- Fetch (`BOJ_FETCH_OUTPUT`): KEY=`code` (ns `boj`, `param_key="code"`), TITLE=`title`,
  DATA=`date`(datetime)+`value`(numeric).
- Enumerate (`BOJ_ENUMERATE_OUTPUT`): KEY=`code` (ns `boj`), TITLE=`title`, METADATA=
  `description`/`db`/`db_title`/`entity_type`/`frequency`/`unit`/`category`/`breadcrumb`/
  `start_date`/`end_date`/`last_update`/`source`.
- Databases search: `db`(KEY)/`title`(TITLE)/`score`(DATA)/`category`+`series_namespace`(METADATA).
- Series search: `code`(KEY ns `boj`)/`title`(TITLE)/`score`(DATA)/`db`(METADATA).

---

## 7. Tests

- `ErrorMappingSuite`: `boj_fetch`, route `…/getDataCode`, `env_key=None` (keyless).
- Offline (`respx`): fetch happy path + null-skipping + EmptyData/Parse/InvalidParameter guards
  + **NEXTPOSITION pagination** (page-1 cursor → page-2 terminal, all series assembled) +
  enumerate breadcrumb/db-row/403-retry + registry floor/shape.
- Integration (live, env-gated): `boj_fetch` (FX, ranged, multi-series, NEXTPOSITION resume,
  unknown→400), bounded `enumerate_boj` (monkeypatched `_list_databases` → 1 DB + request
  counter), `boj_databases_search`/`boj_series_search` over a fixture catalog in `tmp_path`.
- Conformance: `assert_plugin_valid(parsimony_boj)`.
- Catalog probes `catalog_tests/queries.yaml`: `boj_databases` title probes + a per-DB
  `boj_series_<db>` probe.

---

## 8. Live verification log (documentation is a claim; execution is the truth)

| Date | Check | Expected | Actual (live) | Verdict | Action |
|------|-------|----------|---------------|---------|--------|
| 2026-06-09 | DB registry vs manual vs XLSX | 50, agree | manual §II.3.(2) = `api_tool.xlsx` `DB_Name` = hardcoded, **0 diff** | ✅ | registry complete + harvester committed |
| 2026-06-09 | live DB-list endpoint? | — | none (`getMetadata` w/o `db` → 400; `getStatsList`/`getDbList` → 404) | ✅ | archetype C is structural; freeze + cross-validate |
| 2026-06-09 | **Q1: getMetadata returns ALL series per DB?** | uncapped | all 50 DBs return complete RESULTSET, **no NEXTPOSITION on metadata**; CO = 166,513 series in one 99 MB response | ✅ **complete** | per-DB series tier is complete |
| 2026-06-09 | universe size | unknown | **326,466 series** across 50 DBs (Σ getMetadata series rows) | ✅ | catalog target |
| 2026-06-09 | **Q2: NEXTPOSITION truncation on getDataCode** | undocumented behaviour | 22 daily FX series → HTTP 200 "Successfully completed" but only **5 returned**, `NEXTPOSITION=6`; old `boj_fetch` **silently dropped 17 series** | ⚠️→✅ | **fixed**: paginate on NEXTPOSITION |
| 2026-06-09 | NEXTPOSITION resume is lossless | all series | startPosition None→6→11 reassembles **22/22** series, 0 missing, 3 pages | ✅ | series-boundary resume confirmed |
| 2026-06-09 | longest single series vs 60k cap | < 60,000 | `IR01/MADR1Z@D` daily-from-1882 = **52,470 points** (largest) | ✅ | single-series fetch never truncates |
| 2026-06-09 | **Q2: are series fetchable?** | all | one real series from each of 15 DBs (all categories + frequencies) → real values, 15/15 | ✅ | every series reachable by `(db, code)` |
| 2026-06-09 | sample fetch parses real values | numeric | `FM08/FXERD01` → JPY/USD floats in (50,400), real dates | ✅ | — |
| 2026-06-09 | error mapping: unknown code | typed | HTTP 400 → `ProviderError(400)` (not raw httpx) | ✅ | — |

**Completeness sign-off (two-tier, the bls shape):** the **databases catalog is complete**
(50/50, cross-validated against the manual and the machine-readable XLSX); each **per-DB
series catalog is complete** because `getMetadata` is uncapped (proven across all 50 DBs, incl.
the 166k-series CO); and **every series is fetchable** via `boj_fetch(db, code)` now that the
60,000-point `NEXTPOSITION` truncation is paginated. The gap is discovery convenience (the
giant DBs are catalogued per-DB lazily), not access. Deliberate exclusions (`getDataLayer`,
CSV) carry no unique data. Signed: connectors-sweep on 2026-06-09.

---

## 9. Open questions / follow-ups

- [ ] **Publish the catalog snapshots** (build → `validate_catalog` → push `hf://parsimony-dev/boj`).
      Multi-bundle: `boj_databases` + per-DB `boj_series_<db>`. The giant DBs (CO 166k, FF/BIS
      ~34k, PR01 31k, PR03 27k, BP01 18k) are BM25-only builds (no embedding) but heavy
      downloads (CO ≈ 99 MB). Deferred (maintainer step; needs `HF_TOKEN`) — not yet run.
- [ ] A finer `400 → InvalidParameterError` mapping (reading `MESSAGEID` `M181013E` "nonexistent
      code" / `M181005E` "invalid DB") is possible via a per-package mapper but deferred; the
      current `ProviderError(400)` is typed and agent-actionable.
- [ ] `getMetadata` re-harvest cadence: rerun `harvest_databases.py` against `api_tool.xlsx`
      when BoJ revises the manual (it added FF/CO/BIS/DER/OT and dropped the phantom BP02 once
      already).
