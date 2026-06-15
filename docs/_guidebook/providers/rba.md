# Provider dossier: Reserve Bank of Australia (`rba`)

> One file per provider/system. Compiled before/while refactoring `packages/rba`
> through the guidebook process (the bde/bdf/bdp/bls/boc/boj/destatis/eia re-run
> series). Sections 1–3 are the doc compilation; 8 is the live-verification log.
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked

**Provider:** Reserve Bank of Australia  ·  **Key:** `rba`  ·  **Homepage:** https://www.rba.gov.au/statistics/
**Distribution:** `parsimony-rba`  ·  **Namespace(s):** `rba`
**Kind:** public-keyless (Akamai-fronted ⇒ curl_cffi)
**Status:** ✅ verified-live  ·  **Owner:** connectors sweep  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- **What it serves:** Australia's central-bank statistical tables — interest rates,
  exchange rates, monetary aggregates, banking/credit, household & business finance,
  balance of payments, economic activity. Published **only as CSV/XLSX spreadsheets**;
  there is **no JSON/REST API and no machine-readable index** — discovery is by
  scraping the two publication-index HTML pages (archetype **E**).
- **Auth:** none (keyless). But `rba.gov.au` is **Akamai bot-managed** and
  TLS-fingerprint-blocks stock httpx (403) → the connector uses **curl_cffi**
  (`impersonate="chrome"`) as a hard dependency with a hand-written error mapper.
- **Discovery model:** we build a catalog (no native search). 3-pass HTML scrape.
- **Total addressable universe:** **≈4,165 series** = 3,958 (current CSV tables)
  + 7 (one XLSX-exclusive sheet, Bond Purchase Program) + ~200 discontinued series
  in the legacy `xls-hist/*.xls` binaries. (readrba, the canonical R client, counts
  ~4,354 incl. forecast series + a slightly different readable set — same ballpark.)
- **Connectors shipped:** `rba_search`, `rba_fetch`, `enumerate_rba`. (`rba_fetch`
  is **extended this re-run to fetch XLSX-exclusive + xls-hist series**, closing the
  prior "catalog ⊋ connector" gap — see §5/§8.)
- **Completeness verdict:** catalog covers ALL? **YES** (3 passes proven to cover
  every standard series; the only un-catalogued material is genuinely non-time-series
  — individual-bank balance sheets, occasional papers, a USD-repo transaction log —
  which even readrba excludes). connectors cover ALL? **YES** after this re-run
  (every catalogued series is now fetchable; was PARTIAL — CSV-only fetch).
- **Known gaps / deliberate exclusions:** non-standard-layout historical workbooks
  with no `Series ID`/`Mnemonic` row (individual-bank assets/liabilities, occasional
  papers, usd-repos) are catalog-skipped — they expose no addressable series. The
  tables-page `*hist.xlsx` (~70) and the 11 period-range archives are skipped as
  **proven-redundant** (same series ids as the current CSVs, longer history only).

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | Statistical Tables index | https://www.rba.gov.au/statistics/tables/ | web | yes | the current-table publication index — CSV + XLSX links. Archetype-E pass 1+2 source. |
| 2 | Historical Data index | https://www.rba.gov.au/statistics/historical-data.html | web | yes | the discontinued-series index — `xls-hist/*.xls` links. Pass 3 source. |
| 3 | Statistics landing | https://www.rba.gov.au/statistics/ | web | yes | human nav; no machine index |
| 4 | Copyright & Disclaimer | https://www.rba.gov.au/copyright/ | legal | yes | **CC BY 4.0** (see §2) |
| 5 | `readrba` (R client) | https://github.com/mattcowgill/readrba | code | yes | **the canonical reference impl** — mined for the data model + the unreadable-table set |
| 6 | `raustats` (R client) | https://github.com/mitcda/raustats | code | yes | second client; same scrape model |
| 7 | No REST/JSON API | — | — | — | confirmed absent; spreadsheets only |

**No OpenAPI/Swagger, no bulk manifest, no API.** The "client libraries" (`readrba`,
`raustats`) are themselves scrapers of the same two HTML pages — confirming
archetype E is *structural*, not a shortcut.

### 1.2 The data model (in RBA's terms)

- **Atomic fetchable unit:** a **series** (e.g. `FIRMMCRTD` = cash-rate target),
  identified by an alphanumeric **Series ID** (legacy historical workbooks sometimes
  label the same column `Mnemonic`).
- **Hierarchy:** `statistical table` (e.g. **F1** Interest Rates) → one **workbook**
  → one-or-more **sheets** → columns, each column a series. A current table's sheets
  are *also* exported individually as **CSV files** (one CSV per sheet). The workbook
  (XLSX) is the canonical artifact; CSV is a per-sheet convenience export (this is the
  `readrba` model: it reads the XLSX, every data sheet bar `Notes`/`Series breaks`).
- **Per-series metadata** (fixed header block at the top of every CSV/sheet): `Title`,
  `Description` (rich prose — the highest-signal retrieval field), `Frequency`,
  `Type`, `Units`, `Source`, `Publication date`, `Series ID`.
- **Series IDs are reused across closely-related tables** (e.g. the B13.1.x vs B13.2.x
  regional-bank claim breakdowns share ~225 ids each) → the catalog KEY must be
  compound `{table_id}#{series_id}`, never a bare id (a bare id dedups ~5% of entries).

### 1.3 Endpoint reference (there is no API — these are static files)

| "Endpoint" (static path) | Method | Returns | Notes |
|---|---|---|---|
| `/statistics/tables/` | GET html | current-table index | scrape CSV + XLSX `href`s |
| `/statistics/tables/csv/<stem>.csv` | GET csv | one sheet's data + header | the bulk fetch path |
| `/statistics/tables/xls/<stem>.xlsx` | GET xlsx | a full table workbook (all sheets) | current XLSX (5 non-hist + ~70 `*hist.xlsx`) |
| `/statistics/historical-data.html` | GET html | historical index | scrape `xls-hist/*.xls` `href`s |
| `/statistics/tables/xls-hist/<stem>.xls` | GET xls | legacy discontinued-series workbook | xlrd (pre-xlsx binary) |

### 1.4 The "full universe" question

- **Authoritative enumeration path:** the **union of three passes** (no single index
  lists every series):
  1. **CSV index** — scrape `/statistics/tables/` for `csv/*.csv` links (216 files),
     parse each file's `Series ID` header row → **3,958 series**. The bulk.
  2. **XLSX-exclusive sheets** — the 5 current non-hist workbooks
     (`a01,a03,f01d,f02d,f16`) have sheets *not* re-exported as CSV. **Audited live:
     exactly ONE such sheet exists — `a03.xlsx` → "Bond Purchase Program" (7 series).**
     Computed by **dynamic exclusivity** (emit a workbook series only if its id is not
     already in the CSV-covered set) — self-maintaining; replaces a hardcoded
     `{a03: Bond Purchase Program}` allow-list that happened to be correct.
  3. **Legacy `xls-hist/*.xls`** — 26 named binaries on the historical page carry
     **~200 discontinued** series that left the live CSVs (a01hist, a03hist-*,
     b03hist, c09hist, e04–e07hist, f16hist, zcr…). ~200 exclusive (some intra-family
     dups across the a03hist-2003/2009/2012 and f16hist split files).
- **Pagination/recursion:** none — flat index pages; bounded ~250-request fan-out.
- **Estimated total:** **≈4,165 series** (3,958 + 7 + ~200). readrba's
  `browse_rba_series()` reports ~4,354 (includes RBA forecast series + a marginally
  different readable set).
- **Things that exist but are NOT addressable series (correctly excluded):**
  - **`*hist.xlsx` on the tables page (~70 files)** — long-history versions of nearly
    every current table. **Audited 17 across a–j: 0 exclusive series** (identical ids
    to the current CSVs, older dates only). Redundant for *discovery*; skipped.
  - **11 period-range archives** (`1983-1986.xls`…`2023-current.xls`) — historical
    daily exchange-rate bundles. **Audited: 0 exclusive ids** (same currency series).
    Skipped (and the `stem[0].isalpha()` filter already drops them).
  - **Non-standard workbooks** (`hist-assets-indiv-banks`, `hist-liabilities-indiv-banks`,
    `occ-paper-10*`, `usd-repos`) — no `Series ID`/`Mnemonic` row; not time-series.
    Yield 0 rows (the parser returns `[]`); readrba likewise excludes "Occasional Paper".
- **Gated behind a higher plan/login:** none — all public.

---

## 2. Authentication & access

- **Auth required?** No. Keyless. No `secrets=`, no `bind`/`load(api_key)`, no
  `UnauthorizedError` on the data path. `load(*, catalog_url=None)` binds only the
  search catalog URL.
- **Anti-bot:** `rba.gov.au` is **Akamai bot-managed** — stock httpx is
  TLS-fingerprint-blocked (HTTP 403). The canonical `make_http_client`/`fetch_json`
  path **structurally cannot reach this host**. → **curl_cffi** (`impersonate="chrome"`)
  hard dep + the §6 hand-written error mapper. A browser `User-Agent` alone is NOT
  enough — only a real Chrome TLS handshake passes (same wall as `download.bls.gov`).
- **Rate limits:** none documented. Bounded fan-out + one pooled curl_cffi session.
- **Licence (§5 source):** **CC BY 4.0** — reproduce/publish/communicate/adapt,
  incl. commercial, with attribution **"Source: Reserve Bank of Australia"**. Caveats:
  the copyright page's §5 carves out certain **Financial Data** (RBA "does not
  administer … as a benchmark", no endorsement implication) and **third-party content
  (e.g. ABS-sourced series)** needs separate permission. A derived **id+title+description
  discovery catalog** is squarely fine; the caveats bite only on *bulk-data*
  redistribution, which we don't do.

---

## 3. Transport & quirks

- **Base URL:** `https://www.rba.gov.au`
- **Formats:** HTML (index pages), CSV (per-sheet), XLSX (openpyxl), legacy XLS (xlrd).
- **Transport:** raw curl_cffi + hand-written mapper (`_curl_get`): 429→RateLimit
  (+Retry-After), 402→Payment, 401/403→Unauthorized, other≥400→Provider(status);
  curl_cffi Timeout/RequestException→Provider(408). NOT httpx ⇒ kernel `map_http_error`
  doesn't apply (the §6 carve-out; same recipe BLS reused for its flat-file host).
- **Date formats:** `%d-%b-%Y` (`01-Jan-2026`) and `%d/%m/%Y`; normalized to ISO.
- **CSV header block:** fixed-shape metadata rows precede the data; the data section
  starts after the `Series ID` row. Some workbooks have a leading title line.
- **XLSX gotchas:** a few RBA workbooks ship a `[trash]/` folder + a stylesheet
  openpyxl refuses → a raw-zip XML fallback parser exists for the metadata path.
  Computed-range sheets (defined-name `OFFSET(...)`) emit an openpyxl warning (benign).
- **Landmine:** keep the Chrome impersonation target current — old fingerprints start
  to 403.

---

## 4. Catalog plan

- **Strategy:** enumerator + catalog_build (no native search). Archetype **E**.
- **Namespace:** `rba`.
- **Code scheme:** compound `{table_id}#{series_id}` (ids reused across tables).
- **Entity shape:** KEY=`code` (ns `rba`), TITLE=`title`, METADATA=`description`,
  `source`, `table_id`, `series_id`, `category`, `frequency`, `unit`.
- **`source` dispatch column:** `rba_csv` | `rba_xlsx` | `rba_xlsx_hist` — tells the
  agent which fetch path a hit needs (now all three are fetchable by `rba_fetch`).
- **Index policy:** `discovery_indexes()` — `code`=BM25; `title`/`description`
  adaptive (BM25-only above 1000 unique values — title cardinality is high, so
  semantic title probes stay `optional`).
- **Catalog URL:** `hf://parsimony-dev/rba` · env `PARSIMONY_RBA_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Universe slice |
|-----------|-----------|-------|---------|----------------|
| `rba_search` | make_local_search_connector | yes | discovery | whole catalog |
| `rba_fetch` | @connector | no | fetch a table/series by `table_id` | **all three formats** (CSV, XLSX-exclusive sheet, xls-hist) |
| `enumerate_rba` | @enumerator | no | catalog feed | the whole universe (3 passes) |

- **This re-run's change:** `rba_fetch` previously resolved **only** CSV stems, so the
  7 Bond-Purchase-Program (`rba_xlsx`) + ~200 `rba_xlsx_hist` catalogued series were
  *discoverable but not fetchable* — the guidebook's named "catalog ⊋ connector"
  cautionary case. `rba_fetch` now resolves a `table_id` across all three publication
  formats: a `/`-bearing id (`a03/Bond Purchase Program`, `b03hist/<sheet>`) → workbook
  + sheet; a bare id → CSV stem (fuzzy-resolved) else a single-sheet xls-hist stem. A
  shared sheet→long-frame melter handles CSV rows, openpyxl rows, and xlrd rows
  symmetrically.
- **Deliberately NOT wrapped:** the redundant `*hist.xlsx` / period archives (no new
  series) and the non-time-series workbooks (no addressable series). Documented in §1.4.

## 6. Output schemas

- **Fetch `RBA_FETCH_OUTPUT`:** KEY=`table_id` (ns rba), TITLE=`title`,
  DATA=`date`(datetime)/`value`(numeric)/`series_key`.
- **Enumerate `RBA_ENUMERATE_OUTPUT`:** KEY=`code` (ns rba) + TITLE=`title` +
  METADATA(description, source, table_id, series_id, category, frequency, unit).
- **Search `RBA_SEARCH_OUTPUT`:** `code`(KEY)/`title`(TITLE)/`score`(DATA). The search
  description tells the agent to pass the part before `#` to `rba_fetch`.

---

## 7. Tests

- `test_error_mapping_rba.py` — pins the curl_cffi hand-written mapper directly
  (the `ErrorMappingSuite` is httpx/respx-only ⇒ N/A for curl_cffi).
- `test_rba_connectors.py` — offline, mocks **curl_cffi** (`_FakeSession`), not respx:
  CSV happy-path + the new XLSX/xls-hist **fetch** paths, compound-code collisions,
  dynamic XLSX exclusivity, per-fetch error swallowing, bounding seam, status mapping.
- `test_public_surface.py` (new) — `__all__`, exact `CONNECTORS` count, internal seams
  not re-exported as public connectors.
- `test_build_catalog.py` (new) — index types + `default_field`.
- `test_integration_rba.py` — live (curl_cffi), bounded enumerate + fixture-catalog
  search + the new live XLSX-exclusive + xls-hist fetches.
- `catalog_tests/queries.yaml` (new) — recall probes (fixes a dangling registry ref).

---

## 8. Live verification log

| Date | Check | Expected (docs) | Actual (live) | Verdict | Action |
|------|-------|-----------------|---------------|---------|--------|
| 2026-06-09 | CSV index count | "~216 CSVs" | 216 CSV links → 3,958 series ids | ✅ | bulk pass confirmed |
| 2026-06-09 | XLSX-exclusive set | "a03 BPP only" | dynamic exclusivity over all 5 current XLSX → ONLY a03 "Bond Purchase Program" (7) is exclusive | ✅ | replace hardcoded allow-list with dynamic check |
| 2026-06-09 | `*hist.xlsx` exclusive? | unknown (never examined) | 17 sampled a–j → **0 exclusive ids** (same series, longer history) | ✅ | confirmed redundant — keep skipping |
| 2026-06-09 | period archives exclusive? | unknown | 1983-1986/1987-1990 → 0 exclusive (exchange-rate history) | ✅ | confirmed redundant — keep skipping |
| 2026-06-09 | xls-hist named adds series? | "~186 discontinued" | ~200 exclusive across 18 parseable files (a01hist 18, b03hist 28, e04-07 35, f16hist 36…) | ✅ | xls-hist pass is needed |
| 2026-06-09 | non-standard xls-hist | — | indiv-banks / occ-papers / usd-repos → 0 ids (no Series ID row) | ✅ | correctly skipped (matches readrba) |
| 2026-06-09 | Akamai blocks httpx, curl_cffi passes | docs say Akamai | curl_cffi `impersonate=chrome` → 200; (httpx → 403) | ✅ | curl_cffi hard dep confirmed |
| 2026-06-09 | F1 cash-rate fetch real values | policy band 0–8% | FIRMMCRTD real series, >100 obs | ✅ | (live test) |
| 2026-06-09 | **Q2: XLSX-exclusive fetch** | was un-fetchable | `rba_fetch("a03/Bond Purchase Program")` → BPP data | ✅ | gap closed |
| 2026-06-09 | **Q2: xls-hist fetch** | was un-fetchable | `rba_fetch("b03hist")` → discontinued repo data | ✅ | gap closed |

**Completeness sign-off:** the catalog contains all ≈4,165 addressable series across
the three publication formats (CSV 3,958 + XLSX-exclusive 7 + xls-hist ~200), verified
by live audit of every layer + proof that the skipped layers (`*hist.xlsx`, period
archives) add zero new series. Every catalogued series is now fetchable by `rba_fetch`.
Non-time-series workbooks (individual-bank balance sheets, occasional papers, usd-repos)
are the only un-catalogued material and expose no series. Licence CC BY 4.0 permits the
derived discovery catalog with "Source: RBA" attribution.

---

## 9. Open questions / follow-ups

- [ ] Publish the catalog snapshot (`hf://parsimony-dev/rba`) — maintainer step,
  deferred (consistent with the other re-run providers).
- [ ] If RBA ever ships a JSON API, archetype A would replace the 3-pass scrape.
