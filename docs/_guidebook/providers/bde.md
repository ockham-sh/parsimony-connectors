# Provider dossier: Banco de España (`bde`)

> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** Banco de España (Bank of Spain) statistics web service · **Key:** `bde` · **Homepage:** https://www.bde.es
**Distribution:** `parsimony-bde` · **Namespace(s):** `bde`
**Kind:** public-keyless (no API key; BIEST web service is open)
**Status:** ✅ verified-live · **Owner:** (autonomous) · **Last updated:** 2026-06-08

---

## 0. TL;DR

- What this provider serves: Spanish macro/monetary/financial time series (the BIEST "web service") — interest rates, exchange rates, financial accounts, national accounts, bank-lending survey, financial indicators, international economy.
- Auth: **none** (keyless public JSON service). No env var.
- Discovery model: **we build a catalog** (no native search endpoint) from six Spanish-only CSV "chapter" files + the Bank Lending Survey bulk `pb.zip`.
- Total addressable universe: **15,547 unique series** (20,494 raw catalog rows minus 4,947 cross-chapter duplicates). Of these **15,169 (97.6%) are fetchable**; the 262 `pb` families were un-fetchable as catalogued but **recovered (~350 real series) via `pb.zip`**, leaving a ~116-code (~1%) un-fetchable tail (dollar-variant/external-sector codes the web service doesn't serve). Counted by parsing every chapter + an exhaustive live `favoritas` sweep, 2026-06-08.
- Connectors shipped: `bde_fetch` (listaSeries), `enumerate_bde` (catalog feed), `bde_search` (local catalog search).
- Completeness verdict (after this pass): catalog covers ALL fetchable data? **YES** (dedup + pb recovery landed; the ~1% un-fetchable tail is documented and fails cleanly). connectors cover ALL accessible data? **YES** — `listaSeries` is the superset fetch endpoint; `favoritas` adds no unique *data* (latest value + a derived trend arrow), so it is deliberately not wrapped.
- Known gaps / deliberate exclusions: (1) the ~116 un-fetchable straggler codes are left in the catalog and return a clean `InvalidParameterError` on fetch (not auto-filtered — see §9); (2) `favoritas` not wrapped (no unique data); (3) catalog is **bilingual** — English title via `favoritas(idioma=en)` where BdE has one (Spanish title fallback), Spanish description retained and indexed. No English CSV exists, so the English coverage is whatever BdE has translated (the national-accounts bulk is largely Spanish; rates/FX/indicators are translated).

---

## 1. Documentation compilation (THE MOST IMPORTANT STEP)

> §1.1–§1.3 and the deep facets (full API reference, authoritative chapter index,
> data model / families, terms & alternatives, favoritas vs listaSeries) are being
> compiled by a dedicated parallel research pass (workflow `wc9325mjl`) and will be
> synthesized in here. What is below is already live-verified by me.

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | Official API reference (EN) | https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html | api-docs | yes (PROD) | defines `favoritas` + `listaSeries` |
| 2 | Official API reference (ES) | https://www.bde.es/webbe/es/estadisticas/recursos/api-estadisticas-bde.html | api-docs | yes | |
| 3 | Catalog CSV chapters | https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv/catalogo_{be,cf,ie,pb,si,tc,ti}.csv | catalog | yes (last-modified 2026-06-05) | Spanish only; `/en/` and `/ca/` 404 |
| 4 | BIEST series browser | https://app.bde.es/bie_www/ | portal | — | the interactive tool the web service mirrors a subset of |
| … | (more from workflow) | | | | |

### 1.2 The data model (provider's own terms)

- Atomic fetchable unit: a **series** identified by a `serie` code (e.g. `D_1NBAF472` = One-year Euribor).
- Code schemes observed: `D_`-prefixed short codes (`D_1NBAF472`), long structured codes (`DSPC102020WP31000_ES14A_ZU2_TSC.T`), SDMX-style dotted codes (`DEEQ.N.ES.W1.S1.S1...`), `#`-containing codes (must be `%23`-escaped). **`pb` chapter codes (`PB_1_1.1`) are FAMILY/table ids, NOT fetchable series** (see §8).
- Each series carries metadata in the `informacion[]` block: Name, Description, Units, Decimals, #observations, First/Last/Min/Max value, Source, Notes.
- (Hierarchy / Boletín Estadístico chapter→table→series mapping: from workflow.)

### 1.3 Endpoint reference (verbatim — being completed by workflow)

| Endpoint | Method | Required params | Optional | Returns | Notes |
|----------|--------|-----------------|----------|---------|-------|
| `favoritas` | GET | `idioma`, `series` | — | latest value per series (+freq, desc, trend) | placeholder `serie=null` entry for unavailable codes; never 412s |
| `listaSeries` | GET | `idioma`, `series` | `rango` | full series + `informacion[]` metadata + `fechas`/`valores` | **HTTP 412 for the whole batch if ANY code invalid** |

### 1.4 The "full universe" question — how is EVERYTHING listed?

- **Authoritative enumeration path:** the 7 Spanish catalog CSVs at `…/compartido/datos/csv/catalogo_{be,cf,ie,pb,si,tc,ti}.csv`. Live brute-force of a wide candidate code-set confirmed **only these 7 return 200**; all other `catalogo_XX.csv` 302-redirect to a 404 handler. (Authoritative index page confirmation: from workflow.)
- Requires per-chapter fan-out (7 files), CP1252 decode, CSV parse. No pagination.
- Counts (parsed live 2026-06-08 with the connector's own `csv.reader` + 17-col header):
  | chapter | EN name (current code) | valid rows |
  |---|---|---|
  | be | General Statistics | 14,092 |
  | cf | Financial Accounts | 4,732 |
  | si | Financial Indicators | 1,195 |
  | pb | Bank Lending Survey | 262 |
  | ie | International Economy | 93 |
  | tc | Exchange Rates | 71 |
  | ti | Interest Rates | 49 |
  | **Σ** | | **20,494 rows** |
- **Cross-chapter duplicates:** 2,726 codes appear in >1 chapter → **15,547 unique** (4,947 duplicate rows, 24%). `D_1NBAF472` (Euribor) is in 2 chapters; `D_1TFK09A0_BCE` in 3.
- **The "subset" trap (BdE's own words):** the API page states *"The web service only includes a subset of the information published on our website or through the BIEST tool."* Confirmed live: not every catalog code is fetchable (see §8 hit-rates) — the catalog **over-promises** unless filtered.
- Unreachable: the `pb` families (262) cannot be fetched via the web service as-is; recovery path under investigation (workflow `data-model-families`).

---

## 2. Authentication & access

- Auth required? **No.** Open, unauthenticated JSON service. No `secrets=`, no `bind()`, no `UnauthorizedError`. (Correct in current code.)
- Rate limits / anti-bot: none documented or observed; no 429s, no CAPTCHA, no TLS-fingerprint blocking. `robots.txt` does not disallow the CSV path. The enumerator's modest throttle (4 concurrent, 0.25s delay) is courtesy, not required.
- Redistribution: BdE *Aviso legal* (https://www.bde.es/wbe/en/pie/aviso-legal/) is **not** an open-data licence but **permits reuse with attribution** ("the Banco de España shall always be cited as the source"). So publishing a catalog **snapshot** (codes + titles + metadata, NO observation values) to `hf://parsimony-dev/bde` is allowed provided BdE is credited; observation **values** are fetched live, never redistributed.
- ⛔ Human intervention needed? **No** — keyless. Nothing to ask the user for.

---

## 3. Transport & quirks

- Base URLs: API `https://app.bde.es/bierest/resources/srdatosapp` · catalog `https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv`.
- Response formats: API = **gzip-compressed JSON** (must send `Accept-Encoding: gzip` / use `--compressed`; `fetch_json`/httpx handle this automatically). Catalog = **CP1252 CSV** (comma-delimited, quoted, `\r`-ish line endings, embedded newlines in quoted fields → must use a real CSV parser, not line-split).
- Error shape: **HTTP 412** + `{"errNum":412,"errMsgUsr":"Error de validación en la solicitud","errMsgDebug":"La serie X no existe"}` for an invalid/absent series. listaSeries 412s the **entire batch** on one bad code (no partial result).
- Anti-bot: none — plain `curl --compressed` works; no TLS-fingerprint blocking, no `curl_cffi` needed.
- Date format: ISO-8601 with time, `YYYY-MM-DDTHH:MM:SSZ`, **newest-first**. Daily = full date (skips weekends/holidays); monthly = `YYYY-MM-01`; quarterly = first month of quarter (01/04/07/10); annual = `YYYY-01-01`. `bde_fetch` parses this with `pd.to_datetime` before returning (`OutputSpec` never coerces dtypes).
- `rango` is **frequency-dependent** (BdE validates server-side): monthly/quarterly/semestral → `30M`/`60M`/`MAX`/`YYYY`; daily/business-daily → `3M`/`12M`/`36M`/`YYYY` (**not `MAX`**); annual → `60M`/`MAX`/`YYYY`. A multi-series request mixing daily + non-daily under one explicit range 412s — pass a 4-digit year (works for every frequency) or `None`.

---

## 4. Catalog plan (as built)

- Strategy: **enumerator + catalog_build** (crawl 6 CSV chapters + parse `pb.zip` → dedup → `Catalog` snapshot → `make_local_search_connector`). Archetype: **B (crawl hierarchy fan-out)** with a **D (hybrid live+static)** twist for `pb` (bulk-ZIP recovery).
- Namespace: `bde`.
- **Fixes landed (this pass):** (1) **dedup** by `key` (first-chapter-wins) before building entities — removes 4,947 dup rows; (2) **pb recovery** from `pb.zip` (real `DPB…` codes) — the CSV's `PB_1_1.1` aliases never enter the catalog; (3) **bilingual enrichment** in `build_bde_catalog` — English title via `favoritas(idioma=en)` (retry + split-on-failure), Spanish fallback, Spanish description retained; (4) best-effort per-source skip extended to `pb.zip`. Deferred: straggler auto-filter; richer leaf-title fallback (see §9).
- Index policy: `discovery_indexes()` (code=BM25; title/description=adaptive). With ~15.5k unique entries, embedding memory is the consideration.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `bde_search` | @connector (local search) | yes | discovery | the whole catalog |
| `bde_fetch` | @connector | no | series fetch via `listaSeries` | all fetchable series (incl. recovered BLS) |
| `enumerate_bde` | @enumerator | no | catalog feed | 6 CSV chapters + `pb.zip` → unique universe |

- `favoritas` deliberately **NOT wrapped**: confirmed via field-level diff that it exposes no unique *data* — it returns the latest value (already in `listaSeries`'s history) plus a derived `tendencia` (trend arrow `+`/`-`/`=`). Wrapping it would add a redundant connector, not new data coverage. (It does have nicer per-series error semantics — 200 with `serie:null` placeholders vs `listaSeries`'s whole-batch 412 — but that's not a data-coverage gain.)

## 6. Output schemas

- `bde_fetch` → KEY `key`(ns bde) + TITLE `title` + DATA `date`(datetime), `value`(numeric). (Drops the rich `informacion[]` metadata — units/source/notes. Possible enrichment.)
- `bde_search` → KEY `code`(ns bde) + TITLE `title` + DATA `score`.
- `enumerate_bde` → KEY `key`(ns bde) + TITLE `title` + 12 METADATA (description, source, alias, dataset, category, frequency, unit, decimals, start_date, end_date, n_obs, source_org).

## 7. Tests

- Existing: mocked unit tests (respx), `ErrorMappingSuite` (env_key=None), conformance, integration (`bde_fetch` live).
- To add (from completeness work): dedup assertion; pb-exclusion assertion; an integration test that samples N catalog codes and asserts a fetchability floor; date-format-per-frequency assertions.

---

## 8. Live verification log (documentation is a claim; execution is the truth)

> All rows below are MY live executions on 2026-06-08 (not from prior dossiers).

| Date | Check | Expected (docs) | Actual (live) | Verdict |
|------|-------|-----------------|---------------|---------|
| 06-08 | catalog chapter set is exactly 7 | 7 hardcoded | only `be,cf,ie,pb,si,tc,ti` return 200; all other `catalogo_XX` 302→404 | ✅ 7 is complete (pending index-page confirm) |
| 06-08 | catalog universe size | "several thousand" | 20,494 rows / **15,547 unique** | ✅ measured |
| 06-08 | cross-chapter duplicates | (unstated) | **4,947 dup rows (2,726 codes ×≥2)**; enumerate does NO dedup | ⛔ **defect** |
| 06-08 | CSV structure | 17 cols, CP1252 | 17 cols ✓, comma-delimited, CP1252 ✓ | ✅ |
| 06-08 | English catalog exists | — | `/en/` & `/ca/` 404; **Spanish only** | ⚠️ search text is Spanish |
| 06-08 | `listaSeries` fetchability — be | all | 119/120 (99%) | ✅ |
| 06-08 | …cf | all | 116/120 (97%) | ✅ |
| 06-08 | …ie | all | 80/93 (86%) | ⚠️ ~14% un-fetchable |
| 06-08 | …pb | all | **0/262 (0%)** — families, 412 "no existe" | ⛔ **catalog over-promises 262** |
| 06-08 | …si/tc/ti | all | 100% | ✅ |
| 06-08 | error mapping: invalid series | — | **HTTP 412** + errNum JSON; whole batch fails on 1 bad code | ✅ (drives fetch design) |
| 06-08 | metadata richness | key,title,date,value | `informacion[]` has Units/Source/Notes/Min/Max — dropped by fetch (acceptable for a time-series fetch) | ⚠️ noted |
| 06-08 | exhaustive fetchability sweep (all 15,547) | — | **15,169 fetchable / 378 not** (262 pb + ~116 stragglers) | ✅ measured |
| 06-08 | `pb` recovery via `pb.zip` | un-fetchable | `pb.zip` "NOMBRE" row = real `DPB…` codes; **~350 recovered, fetch live ✅** | ✅ **fixed** |
| 06-08 | rango is frequency-dependent | one set | daily rejects `MAX`, takes `3M/12M/36M`; monthly takes `30M/60M/MAX` | ✅ **fixed** (widened + server-validated) |
| 06-08 | invalid series → typed error | — | HTTP 412 now → `InvalidParameterError` with BdE's `errMsgDebug` | ✅ **fixed** |
| 06-08 | redistribution of a catalog snapshot | — | BdE *Aviso legal* permits reuse **with attribution** (not open-data licensed); values must be fetched live, not redistributed | ✅ allowed-with-attribution |
| 06-08 | anti-bot / robots.txt | — | no rate limit, no anti-bot; `app.bde.es` API public; CSV path not disallowed | ✅ |

**Completeness sign-off (2026-06-08):** the catalog contains **all fetchable BdE
BIEST series** — 6 CSV chapters (deduped) + the Bank Lending Survey recovered
from `pb.zip` (~350 series). The fetchable universe is **15,169 + ~350 ≈ 15,500
series**. The ~116 un-fetchable stragglers (≈1%, mostly `…$…` dollar-variant
financial-accounts codes) remain catalogued but return a clean
`InvalidParameterError` on fetch. Connectors expose every accessible data class:
`listaSeries` (full history + metadata) is the superset; `favoritas` carries no
unique data and is deliberately not wrapped. Verified by: full chapter parse +
exhaustive live `favoritas` sweep + live `bde_fetch` of recovered `DPB…` codes +
44 unit / 7 live-integration tests green. Signed: autonomous pass, 2026-06-08.

---

## 9. Open questions / follow-ups

- [x] Authoritative index page that lists the catalog CSVs — `descargas-completas.html` (+ a CSV-structure manual PDF, 2025-03-26); confirms exactly the published set.
- [x] Can the 262 `pb` families be fetched? — **yes, via `pb.zip`** (alias → real `DPB…` map); recovered. The per-family `PB_1_1.csv` paths 404, but the bulk `pb.zip` carries the real codes.
- [x] Redistribution rights — permitted with attribution (BdE *Aviso legal*); catalog metadata only, not observation values.
- [x] **English search text — DONE (bilingual).** Implemented via `favoritas(idioma=en)` (NOT `listaSeries`, which would download full history per series). English title where BdE has one, Spanish fallback; Spanish description retained + indexed. The discovery index is BM25 (>1000 unique → no multilingual embedding), so this enrichment is the *only* thing making any series English-searchable. **Coverage caveat:** a local build measured ~18% English titles, but that build ran during a flaky-network window that silently dropped batches (well-known codes like `DTCCBCEUSDEUR.B`/`D_DTFK09A0` have English in favoritas but didn't get it) — so the enrichment was hardened with retry + split-on-failure, and the true coverage needs a clean-network rebuild to certify. Verified the *logic* is correct: `D_1NBAF472` → "One-year Euribor".
- [ ] **Rebuild + republish the snapshot on a healthy network.** The existing `hf://parsimony-dev/bde` (2026-05-24) is stale/wrong (pre-fix). A local rebuild verified the structural fixes offline (15,635 unique entries, 350 BLS recovered, 0 `PB_` aliases) but BdE began timing out after today's heavy probing (exhaustive sweep + 2 builds), so the bilingual coverage couldn't be re-certified and nothing was published. **Next:** rebuild when BdE is reachable, sanity-check English coverage, then publish (with BdE attribution on the dataset card).
- [ ] **Terse Spanish leaf titles.** For non-enriched rows the `title` falls back to the `split_title_path` *leaf*, which is sometimes an uninformative facet (`"Moneda base: Euro"`, `"TIPO MEDIO"`). The full Spanish text is in `description` (indexed), so search still works, but a richer fallback (description when the leaf is just a facet) would improve display + lexical recall. Pre-existing; not addressed this pass.
- [ ] **Straggler auto-filter.** ~116 catalogued-but-un-fetchable codes could be dropped at build time via a `favoritas` availability pass (`scripts/build_catalog.py --verify-fetchable`). Deferred — they fail cleanly today, and a live filter couples the build to API availability (risk of dropping real series on a transient hiccup).
- [ ] **Normal-chapter ZIP coverage.** Spot-check whether `be`/`cf` topic ZIPs contain series absent from their CSVs (low probability; the CSVs are 99–100% fetchable and are BdE's documented catalog).
