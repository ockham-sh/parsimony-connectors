# Provider dossier: Banque de France (`bdf`)

> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** Banque de France — Webstat statistics API · **Key:** `bdf` · **Homepage:** https://www.banque-france.fr
**Distribution:** `parsimony-bdf` · **Namespace(s):** `bdf`
**Kind:** public-keyed (free Opendatasoft API key, `Authorization: Apikey` header)
**Status:** ✅ verified-live · **Owner:** (autonomous) · **Last updated:** 2026-06-08

---

## 0. TL;DR

- What this provider serves: French macro / monetary / financial time series — exchange rates, monetary aggregates, interest rates, balance of payments, securities, inflation expectations, business surveys, etc. (the Webstat universe).
- Auth: **free API key**, sent `Authorization: Apikey <KEY>` (literal word `Apikey`, not `Bearer`); env var `BDF_API_KEY`. Quota 10,000 req/day.
- Discovery model: **we build a catalog** (no native keyword-search verb). Webstat is an Opendatasoft Explore v2.1 instance; the `series` system-dataset is a single flat table listing every series, so enumeration is **archetype A** (one streamed export).
- Total addressable universe: **41,641 series** across **45 dataflows** (live `series` `total_count`, 2026-06-08). Counted from `GET /catalog/datasets/series/records?limit=1` `total_count`.
- Connectors shipped: `bdf_fetch` (observations), `enumerate_bdf` (full-index catalog feed), `bdf_search` (local catalog search).
- Completeness verdict: catalog covers ALL series? **YES** (the `series` export *is* the authoritative universe; `len(catalog) - 45 stubs` == `series` total_count). Connectors cover ALL accessible data classes? **YES** — `observations` is the only value-bearing endpoint; the other system datasets are metadata the catalog already folds in.
- Known gaps / deliberate exclusions: the 16 other Opendatasoft system datasets (`themes`, `codelists`, `sources`, `glossary`, …) are reference/metadata, not time-series data — deliberately not wrapped as fetch verbs (their useful content rides in the catalog as titles/paths/descriptions).

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | Webstat developer portal | https://developer.webstat.banque-france.fr/ | portal | yes | key registration + API docs |
| 2 | Opendatasoft Explore API v2.1 | `https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets` | api-docs | yes | standard ODS Explore surface (`/records`, `/exports/json`, ODSQL `where`/`select`/`refine`/`order_by`) |
| 3 | Webstat portal | https://webstat.banque-france.fr | web | yes | human-facing browser the API mirrors |

### 1.2 The data model (provider's own terms)

- Atomic fetchable unit: a **series**, identified by a dot-separated SDMX `series_key` (e.g. `EXR.M.USD.EUR.SP00.E`; the leading token is the dataflow id, so keys are **globally unique** — no compound code needed).
- Hierarchy: **dataflow (`dataset_id`) → series**. 45 dataflows. Each series also carries a `path_en`/`path_fr` topic breadcrumb (e.g. `['Rates and prices/Market interest rates']`).
- Metadata per series (flat columns on the `series` table): bilingual `title_en`/`title_fr`/`title_long_en`/`title_long_fr`, `freq`, `ref_area`, `currency`, `unit`, `decimals`, `source_agency`, `first_time_period_date`, `last_time_period_date`, plus a wide sparse set of SDMX dimension columns (mostly null per row).

### 1.3 Endpoint reference (live-verified)

| Endpoint (relative to `…/catalog/datasets`) | Method | Key params | Returns | Notes |
|---|---|---|---|---|
| `?limit=N&select=dataset_id` | GET | — | the **17** Opendatasoft system datasets | not the 45 dataflows — see §1.4 trap |
| `webstat-datasets/exports/json` | GET | `select` | 45 dataflow stub rows (`dataset_id`, bilingual `name`/`description`, `series_count`, `paths_*`) | one response, no pagination |
| `series/exports/json` | GET | `select`, optional `refine=dataset_id:X` | the full ~41.6k-series flat table (or one dataflow) | **the authoritative enumeration path** |
| `observations/exports/json` | GET | `where=series_key="…"`, `select`, `order_by` | observation rows (`time_period_start`, `obs_value`, …) | `obs_value` null on `OBS_STATUS=M` gaps |

### 1.4 The "full universe" question

- **Authoritative enumeration path:** the `series` system-dataset exported in full (`series/exports/json` with a lean `select`). One streamed call → every addressable unit. **Archetype A.**
- Pagination/recursion? No. `/exports/json` streams the whole (optionally `refine`-filtered) set in one response.
- Estimated total: **41,641 series** (live `series` `total_count`). The 45 dataflow stubs are added as synthetic `dataset:{id}` parent rows for navigation.
- **The "17 vs 45" trap:** `GET /catalog/datasets` reports `total_count: 17` — those are the Opendatasoft *system* datasets (`series`, `observations`, `webstat-datasets`, `themes`, …), **not** the 45 BdF dataflows. The 45 live inside the `webstat-datasets` table. Reading the catalog count as "datasets" would badly undercount; the real universe is in `series`.
- Unreachable / gated: none observed at the free tier; the full export and observations both return at the supplied key.

---

## 2. Authentication & access

- Auth required? **Yes** — free Opendatasoft API key, header `Authorization: Apikey <KEY>` (literal `Apikey`). Wrong scheme (`Bearer`) → silent 401.
- Obtain: register at the Webstat developer portal (§1.1 #1).
- Limits: 10,000 requests/day. The whole catalog builds in ~2 requests, so the cap is a non-issue.
- Secret handling: `secrets=("api_key",)` on every keyed verb; env fallback `BDF_API_KEY`; fast-fail `UnauthorizedError("bdf", env_var="BDF_API_KEY")` before any network call. Key rides the header (not redacted-by-name in query logs because it never enters a query string).

---

## 3. Transport & quirks

- Base URL: `https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets`. `HttpClient` builds `f"{base}/{path}"`, so the per-endpoint `…/exports/json` paths concatenate correctly.
- Response format: JSON. `/exports/json` returns a **bare array** of row objects (not a `{results: […]}` envelope — that is the `/records` shape). The connector parses the array directly.
- Pagination: none for `/exports/json` (full stream). Fetch uses `where` to bound server-side.
- Anti-bot / TLS-fingerprinting: none observed; stock `httpx` works.
- Date format: `YYYY-MM-DD` (`time_period_start`). `obs_value` is `null` on missing-status observations — values may legitimately be `None` between real points.
- Landmine: the `series` table is ~200 columns wide and sparse; without a lean `select=` the full export is hundreds of MB. The connector selects ~13 columns → a few MB.

---

## 4. Catalog plan (as built)

- Strategy: **enumerator + catalog_build**, archetype **A** (live full-index export). Two requests: `webstat-datasets` (45 stubs) + `series` (full universe).
- Namespace: `bdf`. Code scheme: bare `series_key` for series; synthetic `dataset:{dataset_id}` for the 45 stubs (split by KEY prefix or the `entity_type` column).
- Entity shape: KEY=`code` (ns `bdf`), TITLE=`title` (English, FR/long/key fallback), METADATA=[`description` (bilingual + breadcrumb + dataset context), `entity_type`, `dataset_id`, `frequency`, `ref_area`, `source_agency`, `path`, `first_time_period`, `last_time_period`].
- Index policy: `discovery_indexes()` — `code`=BM25, `title`/`description`=adaptive (BM25-only above 1000 unique → no multilingual embedding bridge, hence the bilingual `description`).
- Multi-bundle? No — one `bdf` bundle (~41.7k rows) is tractable.
- Catalog URL: `hf://parsimony-dev/bdf` · env override `PARSIMONY_BDF_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `bdf_search` | @connector (local search) | yes | discovery | the whole catalog |
| `bdf_fetch` | @connector | no | observations fetch | every series (the one value-bearing endpoint) |
| `enumerate_bdf` | @enumerator | no | catalog feed | the full 41.6k-series universe + 45 stubs |

- Endpoints deliberately NOT wrapped: the 16 non-`series`/`observations` system datasets (`themes`, `codelists`, `sources`, `glossary`, `keyfigures`, …) — reference/metadata, not time-series values. Their signal (titles, paths) already enriches the catalog.

## 6. Output schemas

- `bdf_fetch` → KEY `key`(ns bdf) + TITLE `title` + DATA `date`(datetime), `value`(numeric).
- `bdf_search` → KEY `code`(ns bdf) + TITLE `title` + DATA `score`.
- `enumerate_bdf` → KEY `code`(ns bdf) + TITLE `title` + 9 METADATA (see §4).

## 7. Tests

- `test_bdf_connectors.py` — offline respx: fetch happy/null-value/empty/parse/invalid-param/no-key/env-fallback; enumerate bounded shape + bilingual metadata + best-effort degradation (series-only / stubs-only / empty) + seam monkeypatch; search rank/discriminate/empty.
- `test_error_mapping_bdf.py` — `ErrorMappingSuite` (keyed, default `env_key="api_key"`), route `observations/exports/json`.
- `test_integration_bdf.py` — live: keyed fetch (real FX magnitudes), dataset-bounded enumerate via the `_list_all_series` seam, fixture-catalog search.
- `test_build_catalog.py` — index types + `default_field`.
- `catalog_tests/queries.yaml` — recall gate (exact `code:` + a distinctive `title_bm25` required; semantic `hybrid_title` optional).

---

## 8. Live verification log

> All rows MY live executions on 2026-06-08 against the production Webstat API.

| Date | Check | Expected (docs) | Actual (live) | Verdict |
|------|-------|-----------------|---------------|---------|
| 06-08 | key works (`Authorization: Apikey`) | 200 | **HTTP 200** on `/catalog/datasets?limit=1` | ✅ |
| 06-08 | catalog dataset count | "45 datasets" (old code) | `/catalog/datasets` total_count = **17** (system datasets); 45 lives in `webstat-datasets` | ⚠️ trap mapped |
| 06-08 | series universe size | "~41,607" (old docstring) | `series` total_count = **41,641** | ✅ measured |
| 06-08 | `series/exports/json` honours `select`+`refine` | — | yes — full filtered set, no pagination; `path_en` returned as a JSON array | ✅ |
| 06-08 | fetch path `observations/exports/json` `where`/`order_by` | — | yes; `obs_value` null on `OBS_STATUS=M`; 24 monthly rows for EXR 2022–23 | ✅ |
| 06-08 | fetch real content (EXR.M.USD.EUR.SP00.E) | FX rate | values 0.97–1.12, datetime/float dtypes, EN title | ✅ |
| 06-08 | bounded enumerate (PAI, 3 series) | exact columns + real metadata | 45 stubs + 3 series, exact column match, bilingual desc + breadcrumb path | ✅ |
| 06-08 | catalog build + search over new schema | rank + discriminate | EXR top for "dollar euro", ICP for "consumer prices" | ✅ |
| 06-08 | **Q1 full-export count == universe** | 41,641 | full `series/exports/json` returned **41,641 rows, 0 dups, all 45 dataflows** — exact match to total_count | ✅ measured |
| 06-08 | **Q2 fetchability sweep** | all fetchable? | **545/545 (100%)** have observations: 45 stratified (1/dataflow) + **500 random** across the universe; 0 zero-obs, 0 errors | ✅ measured |

**Completeness sign-off (2026-06-08):** the catalog contains **all 41,641 BdF
series** — confirmed by streaming the full `series/exports/json` and matching its
row count (41,641, zero dups, all 45 dataflows) against the live `total_count`;
the catalog is built directly from that export, so it *is* the authoritative
universe, plus 45 dataflow navigation stubs. **Fetchability measured by sample,
not exhaustively:** a 545-series probe (45 stratified + 500 random) was 100%
fetchable, so any unfetchable tail is < ~0.6% at 95% confidence (rule of three) —
and structurally unlikely, because the `series` table directly backs the
`observations` table in the same Opendatasoft instance (contrast BdE, whose CSV
catalog carried un-fetchable family aliases). The only caveat to "ALL possible
series": the universe is exactly what BdF publishes through the Webstat ODS
`series` table — there is no separate bulk-only surface (no treasury-trap). The
fetch verb covers the only value-bearing endpoint (`observations`);
reference/metadata system datasets are folded into the catalog, not wrapped.
Verified by: live key probe + full data-model mapping + full-export count diff +
545-series fetchability sweep + live `bdf_fetch` + 34 offline / 3 live-integration
tests green + ruff/mypy/`parsimony list --strict` clean. Signed: autonomous pass,
2026-06-08.

---

## 9. Open questions / follow-ups

- [x] **Universe completeness (Q1) + fetchability (Q2) measured.** Full export = 41,641 rows = total_count (0 dups, 45 dataflows); 545-series fetchability sweep = 100%. See §8.
- [ ] **Build + publish the snapshot.** Enumeration completeness is now proven; what remains is the maintainer build-and-publish: a full build of all 41.6k series → embed → `validate_catalog.py` (expect `required_recall 1.00`) → push to `hf://parsimony-dev/bdf`. Not run in this pass (heavy embed; one-time op).
- [ ] **Consider exposing `dim_*` facets as structured METADATA.** The `series` table carries rich SDMX dimensions (currency, instrument, counterpart sector); only `freq`/`ref_area` are surfaced today. Adding a few high-signal ones as indexed METADATA would enable `FIELD:value` structured search — weigh against embedder cost.
- [ ] **`.env` var name.** `BDF_API_KEY` now mirrors `BANQUEDEFRANCE_KEY`; if anything else still reads the latter, keep both in sync (or migrate callers to `BDF_API_KEY`).
