# Provider dossier: US Bureau of Labor Statistics (`bls`)

> One file per provider/system. Compiled before the refactor; §8 is the
> live-verification log that turns "documented" into "proven".
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** US Bureau of Labor Statistics  ·  **Key:** `bls`  ·  **Homepage:** https://www.bls.gov
**Distribution:** `parsimony-bls`  ·  **Namespace(s):** `bls`, `bls_surveys`, `bls_series_<survey>`
**Kind:** public-keyed (key OPTIONAL — raises quota, does not gate data)
**Status:** ✅ verified-live  ·  **Owner:** connectors  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- **What it serves:** the entire US official labor-statistics universe — CPI, PPI,
  employment (CES/CPS/SM/QCEW), unemployment (LAUS), JOLTS, ECI/ECEC, productivity,
  import/export prices, occupational employment/wages, injury/illness, time-use — as
  numeric time series.
- **Auth:** optional `registrationkey` (env `BLS_API_KEY`). Keyless works; a key raises
  the daily quota and request size. **The data API host is plain HTTPS/httpx-friendly;
  the bulk flat-file host `download.bls.gov` is Akamai-bot-walled and needs curl_cffi
  Chrome impersonation** (same wall as `rba`).
- **Discovery model:** **we build a catalog**, but the universe is too large to embed
  whole, so it is **two-tier, exactly like `sdmx`**: a small always-built *surveys*
  catalog (tier 1) plus *per-survey series* catalogs (tier 2) built for the headline
  surveys and lazy-buildable for the rest.
- **Total addressable universe:** **~tens of millions of series** — the per-survey
  `.series` flat files total **15.6 GB** of metadata (counted by summing every
  `<survey>.series` file size on `download.bls.gov/pub/time.series/`). The four biggest
  (`ca`/`cb`/`cs`/`ch` = injury/illness demographic microdata) alone are ~12 GB.
- **Connectors shipped:** `bls_fetch`, `enumerate_bls_surveys`, `enumerate_bls_series`,
  `bls_surveys_search`, `bls_series_search` (5 — two enumerators + 1 fetch + 2 search,
  the sdmx surface).
- **Completeness verdict:** catalog covers ALL? **PARTIAL by design** — the *surveys*
  catalog is 100% complete (every survey discoverable); *series* catalogs are complete
  **per built survey** (a `.series` file is the authoritative full list for its survey),
  published selectively. connectors cover ALL? **YES** — every series in the universe is
  *fetchable* by id via `bls_fetch`; the gap is discovery, not access.
- **Known gaps / deliberate exclusions:** (1) the GB-scale microdata surveys
  (`ca`,`cb`,`cs`,`ch`,`oe`,`nw`,`fw`,`is`,`wm`,`fi`,`ii`,`fa`) are not pre-published as
  series catalogs (they are reachable by id-construction + `bls_fetch`, and their
  dimension vocabularies surface via the tier-1 manifest); (2) dimension-**label**
  resolution is partial on a few surveys (LA 60%, WP 70% — irregular column→table naming);
  unresolved values fall back to the raw code, so titles + fetch are unaffected (§8). Left
  documented, not fixed.

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | Public Data API v2 | https://www.bls.gov/developers/api_signature_v2.htm | api-docs | yes | POST JSON; optional key |
| 2 | Developer portal | https://www.bls.gov/developers/ | portal | yes | key registration link |
| 3 | Flat-file download site | https://download.bls.gov/pub/time.series/ | bulk | yes | **the authoritative full universe**; Akamai-walled |
| 4 | Per-survey overview `<survey>.txt` | …/pub/time.series/`<sv>`/`<sv>`.txt | spec | yes | series-id format + field defs + data-element dictionary |
| 5 | Series-ID format help | https://www.bls.gov/help/hlpforma.htm | web | yes | per-survey id composition grammar |
| 6 | Registration (key) | https://data.bls.gov/registrationEngine/ | portal | yes | free; email |
| 7 | API FAQ / limits | https://www.bls.gov/developers/api_faqs.htm | ops | yes | 25 series/req keyless, 50 keyed; 500 req/day keyed, 25 keyless |

### 1.2 The data model

- **Atomic fetchable unit:** a **series** (a single time series of numeric observations).
- **Identified by:** a `series_id` — a fixed-width code **composed of dimension codes**
  per the survey's grammar. E.g. CPI `CUUR0000SA0` = survey `CU` + seasonal `U` +
  periodicity `R` + area `0000` + item `SA0`. CES `CES0000000001` = `CE` + supersector
  `00` + industry `00000000` + datatype `01`.
- **Hierarchy above it:** **survey** (≈ a "program", e.g. CU = CPI All-Urban) → **series**.
  A survey owns a set of **dimension code tables** (area, item, industry, occupation,
  datatype, seasonal, periodicity, …) and a series-id grammar that combines them.
- **Metadata per series (from the `.series` flat file):** the dimension codes, a
  ready-made `series_title` (present in most surveys), `begin_year`/`begin_period`,
  `end_year`/`end_period`, footnote codes.
- **Frequencies:** monthly (`M01`–`M12`, `M13`=annual avg), quarterly (`Q01`–`Q05`),
  semiannual (`S01`–`S03`), annual (`A01`). Period→date handled in `bls_fetch`.

### 1.3 Endpoint reference

| Endpoint | Method | Required | Optional | Returns | Pagination | Notes |
|----------|--------|----------|----------|---------|------------|-------|
| `api.bls.gov/publicAPI/v2/timeseries/data/` | POST | `seriesid[]`, `startyear`, `endyear` | `registrationkey`, `catalog`, `calculations`, `annualaverage` | obs per series | none (≤20yr/req) | **logical failure in body** (HTTP 200 + `status`) |
| `api.bls.gov/publicAPI/v2/surveys` | GET | — | `registrationkey` | 70 survey abbrev+name | none | tier-1 names |
| `api.bls.gov/publicAPI/v2/timeseries/popular` | GET | — | `survey` | ≤top series | none | **shallow** — popular only, not a full list |
| `download.bls.gov/pub/time.series/` | GET | — | — | HTML dir listing | dir tree | Akamai → curl_cffi |
| `download.bls.gov/pub/time.series/<sv>/<sv>.series` | GET | — | — | TSV: id + dims + title + dates | one file | **authoritative per-survey series list** |
| `download.bls.gov/pub/time.series/<sv>/<sv>.<dim>` | GET | — | — | TSV code→label | one file | dimension/mapping tables |

### 1.4 The "full universe" question

- **Authoritative enumeration path:** the per-survey `.series` flat files at
  `download.bls.gov/pub/time.series/<survey>/<survey>.series`. Each is a TSV listing
  **every** series in that survey with its dimension codes + (usually) a `series_title`
  + active date range. The union of all `<survey>.series` files IS the full universe.
- **Pagination / fan-out:** one file per survey (~65 surveys). No within-file pagination
  (whole-file download). The API has **no** "list all series" endpoint — `timeseries/popular`
  is the only API listing and it is the cautionary shallow case the old connector used.
- **Estimated total:** **~tens of millions of series; 15.6 GB of `.series` metadata.**
  Measured by summing every `<survey>.series` byte size live (§8).
- **The treasury-trap here is inverted:** nothing is *hidden* — everything is in the flat
  files — but the universe is too big to embed. The microdata tail (`ca`/`cb`/`cs`/`ch`
  injury demographics, `oe` occupational cross-products) is real, fetchable, and
  enormous. We catalog it structurally (dimension manifest), not series-by-series.
- **Nothing is gated behind login.** The key only raises quota.

---

## 2. Authentication & access

- **Auth required?** No (optional). **Mechanism:** `registrationkey` in the POST body.
- **Obtain a key:** https://data.bls.gov/registrationEngine/ (free, email). Already present
  in `ockham/.env` as **`BLS_API_KEY`** (32 chars).
- **Limits:** keyless = 25 queries/day, 25 series/query, 10yr span; keyed = 500
  queries/day, 50 series/query, 20yr span. Threshold breach → HTTP 200 with a body
  message ("daily threshold has been reached") → mapped to `RateLimitError`.
- **Human intervention:** none needed — key already provisioned.
- **Secret handling:** `secrets=("api_key",)`, env fallback `BLS_API_KEY`, **never
  fast-failed** (key is optional). The download host is keyless; its Akamai wall is
  defeated with a non-secret browser impersonation, not a credential.

---

## 3. Transport & quirks

- **Base URLs:** API `https://api.bls.gov/publicAPI/v2`; bulk `https://download.bls.gov/pub/time.series`.
- **Formats:** API = JSON (POST). Flat files = **tab-separated** with a header line.
- **`fetch_json` usable?** Yes for the API GETs (`surveys`); `bls_fetch` POSTs JSON via a
  hand POST helper (`fetch_json` is GET-only). The flat-file host needs **raw curl_cffi**
  (Akamai TLS-fingerprint wall — stock httpx/curl get HTTP 200 "Access Denied" pages).
- **Pagination:** none on the API data path; flat files are whole-file.
- **Anti-bot:** `download.bls.gov` is **Akamai bot-managed** — a plain Chrome `User-Agent`
  is *not* enough; only a real Chrome TLS handshake (curl_cffi `impersonate="chrome"`)
  passes. The API host (`api.bls.gov`) is *not* walled — plain httpx works.
- **Dual-meaning status:** the API returns **HTTP 200 with a `status` field** for logical
  failure (`REQUEST_NOT_PROCESSED`). Must inspect the body, not the HTTP code.
- **Date/number traps:** `M13`=annual avg, `Q05`=annual, `S03`=annual; values can be `-`
  (suppressed) → null.

---

## 4. Catalog plan (sdmx-style two-tier)

- **Strategy:** two-tier — tier 1 *surveys* (always built, complete), tier 2 *per-survey
  series* (built for headline surveys, lazy-buildable + LRU-cached otherwise). Directly
  mirrors `parsimony-sdmx` (`agency`→`survey`, `dataflow`→`survey`, DSD codelists→
  dimension tables, series key→`series_id`).
- **Namespaces:** `bls_surveys` (tier 1); `bls_series_<survey>` per survey (tier 2).
- **Code scheme:** tier-1 code = survey abbreviation (`CU`); tier-2 code = the full
  `series_id`.
- **Tier-1 entity:** KEY=`code` (survey, ns `bls_surveys`), TITLE=survey name,
  METADATA=[survey, has_series_catalog, series_id_format, `dimensions` manifest].
- **Tier-2 entity:** KEY=`series_id`, TITLE=`series_title` (or composed from dimension
  labels for the title-less surveys SM/JT/PR), METADATA=[survey, begin_year, end_year,
  per-dimension `<dim>` label + `<dim>_code`].
- **Index policy:** custom (`catalog_policy.py`), index kind by role. Series catalogs:
  `code` and `title` BM25-only (both are per-series row text — a vector buys nothing there),
  each `<dim>` label field hybrid (BM25 + vector) for semantic label matching. Survey catalog:
  `code` BM25, `title` hybrid. Structured `FIELD: value` clauses are the primary retrieval path.
- **Dimension manifest:** compact `[{id, values:[{code,label}…]}]` per survey, attached to
  the tier-1 entity (reuse the sdmx manifest shape) so an agent can navigate codes /
  construct an id even for non-published surveys.
- **Headline-survey allowlist (pre-published + lazy-buildable):** CU, CW, CE, SM, LN, LA,
  JT, WP, PC, ND, EI, CI, EC, PR, MP, BD, CC, AP (+ extensible).
- **Catalog URL:** `hf://parsimony-dev/bls` (subdirs per namespace); env override
  `PARSIMONY_BLS_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `bls_fetch` | @connector | no | fetch observations by series_id(s) via the API | **every** series (by id) |
| `enumerate_bls_surveys` | @enumerator | no | tier-1 feed: one row per survey | all surveys |
| `enumerate_bls_series` | @connector (dynamic schema) | no | tier-2 feed: one row per series in ONE survey | that survey's full universe |
| `bls_surveys_search` | @connector | yes | search surveys + read dimension manifest | all surveys |
| `bls_series_search` | @connector | yes | search one survey's series (structured) | built/allowlisted surveys |

- **Deliberately NOT wrapped:** `timeseries/popular` (shallow, superseded by the flat-file
  enumeration); per-survey series catalogs for the GB-scale microdata surveys (reachable
  by construction + fetch).

## 6. Output schemas

- **`bls_fetch`:** KEY=`series_id` (ns bls), TITLE, METADATA=`frequency`, DATA=`date`
  (datetime) + `value` (numeric).
- **`enumerate_bls_surveys`:** KEY=`code`(ns bls_surveys), TITLE, METADATA=survey,
  has_series_catalog, series_id_format, dimensions.
- **`enumerate_bls_series`:** KEY=`code`(=series_id, per-survey ns), TITLE, METADATA=`*`
  (wildcard — dynamic per-survey dimension columns), like sdmx.
- **`bls_surveys_search`:** code/title/score + survey + dimensions.
- **`bls_series_search`:** series_id/title/score + survey + namespace.
- **Dispatch:** search → `series_id` → `bls_fetch(series_ids=…, start_year, end_year)`.

## 7. Tests

- `ErrorMappingSuite`: route `…/timeseries/data/`, POST, `env_key=None` (key optional).
- Integration (live, `-m integration`): API fetch of `LNS14000000`; live curl_cffi pull
  of one survey's `.series` (bounded); two-tier build of one headline survey; search.
- Conformance: `assert_plugin_valid`.
- `catalog_tests/queries.yaml`: code probes (exact series_id) + title BM25 + structured
  dimension clause.

---

## 8. Live verification log

| Date | Check | Expected | Actual (live) | Verdict | Action |
|------|-------|----------|---------------|---------|--------|
| 2026-06-09 | API `/surveys` count | "~60+" | **70 surveys** | ✅ | tier-1 source |
| 2026-06-09 | `download.bls.gov` w/ Chrome UA (stock httpx/curl) | 200 | **"Access Denied" (Akamai)** | ⚠ | needs curl_cffi |
| 2026-06-09 | `download.bls.gov` w/ curl_cffi impersonate=chrome | 200 | **HTTP 200**, dir HTML | ✅ | transport chosen |
| 2026-06-09 | sum of all `<survey>.series` sizes | "large" | **15.6 GB; ca 4.5G, cb 3.9G, cs 2.1G, ch 1.6G, oe 1.26G, nw 1.25G** | ✅ | full embed infeasible → two-tier |
| 2026-06-09 | `.series` carries a ready-made title | unknown | **Y for CU/CW/LA/LN/WP/PC/ND/OE/CI/EI/BD/AP; N for SM/JT/PR** | ✅ | compose titles for the N set |
| 2026-06-09 | `<survey>.txt` documents the id grammar | claim | **yes — §4 series-file format + §6 mapping files + §7 data-element dictionary** | ✅ | id-construction is documented |
| 2026-06-09 | parsed series rows == raw `.series` data lines (no silent truncation) | equal | **exact on CU 8,104 / CE 22,049 / JT 2,060 / LA 33,985 / WP 5,322 / SM 22,927** | ✅ | per-survey key completeness proven |
| 2026-06-09 | built catalog entity count == authoritative row count (CU) | 8,104 | **8,104 entities** | ✅ | build keeps every key |
| 2026-06-09 | catalog→fetch round trip (sampled id per survey) | each fetches | **CU/CE/JT/LA/WP/SM all return obs** (incl. title-less JT/SM + a discontinued WP series ending 1974) | ✅ | by-id fetch is universal across surveys |
| 2026-06-09 | dimension **label**-resolution coverage | high | **CU/CE/JT/SM 100%; LA 60%; WP 70%** | ⚠ | partial on some surveys — see limitation below |

**Known limitation — partial dimension-label resolution (LA, WP, and similar).** The
`<dim>_code`→label resolver matches a series column to a mapping table by suffix
(`area_code`→`<sv>.area`). BLS's naming is irregular for a few dimensions (e.g. LA's
`srd_code` maps to the table `state_region_division`, not `srd`), so those codes fall back
to the raw code instead of a word label. Impact is contained: it only degrades *structured
search on those specific dimensions* — the `series_title` (full human text, present on
LA/WP) and `bls_fetch` are unaffected, and the title-less surveys that *depend* on labels
for their composed titles (JT, SM) resolved 100%. **Left documented, not fixed** (a full fix
needs each survey's `.txt` data-dictionary column→table map). Decision: 2026-06-09.

**Completeness sign-off:** the **surveys** catalog contains ALL surveys (tier-1, complete);
**series** catalogs are complete *per built survey* (the `.series` file is the authoritative
full list — parsed/built counts match it exactly, verified above) and published selectively
for the headline surveys; **every** series in the universe is reachable by `bls_fetch` (id
access is total, round-trip-verified across 6 diverse surveys). The unbounded microdata tail
is deliberately not pre-embedded — it is reachable by id construction (tier-1 dimension
manifest) + fetch. Caveat: dimension-label resolution is partial on a few surveys (above).
Signed: connectors on 2026-06-09.

---

## 9. Open questions / follow-ups

- [ ] Operator publish job (**deferred — not yet run, by request 2026-06-09**): build + push
  tier-1 `bls_surveys` and tier-2 `bls_series_<survey>` for the headline allowlist to
  `hf://parsimony-dev/bls` (`scripts/build_catalog.py`), then `validate_catalog.py`.
- [ ] **Partial dimension-label resolution (LA/WP and similar) — left documented, not fixed**
  (decision 2026-06-09). A full fix would parse each survey's `.txt` data dictionary for the
  exact column→table map (e.g. LA `srd_code`→`state_region_division`). Until then, unresolved
  codes fall back to the raw code; titles + fetch are unaffected (see §8 limitation note).
- [ ] Decide whether to compose richer titles for title-less surveys (SM/JT/PR) by joining
  the full dimension-table set vs. the minimal label concat.
- [ ] Consider a bounded on-demand build cap for non-allowlisted surveys (size guard +
  helpful "construct an id" error) so `bls_series_search` never tries to index a GB file.
