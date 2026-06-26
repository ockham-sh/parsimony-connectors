# Provider dossier: Banco de Portugal (`bdp`)

> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** Banco de Portugal — BPstat statistics API · **Key:** `bdp` · **Homepage:** https://www.bportugal.pt
**Distribution:** `parsimony-bdp` · **Namespace(s):** `bdp`
**Kind:** public-keyless (open API, no auth)
**Status:** ✅ verified-live · **Owner:** (autonomous) · **Last updated:** 2026-06-08

---

## 0. TL;DR

- What this provider serves: Portuguese macro / monetary / financial / external time series — national financial accounts, MFI balance sheets, balance of payments, interest rates, coincident indicators, securities, etc. (the BPstat universe).
- Auth: **none** — keyless public JSON API (no `secrets=`/`bind()`/`load()`/`UnauthorizedError`).
- Discovery model: **we build a catalog** (no native keyword-search verb). BPstat exposes a JSON-stat hierarchy `domain → dataset → series`; the only way to list every series is to crawl it — **archetype B** (hierarchy crawl), optimised hard.
- Total addressable universe: **72,063 series** across **212 datasets** in **65 leaf domains** (live, 2026-06-08; the `num_series` on each `/domains/` entry sums to this, and each dataset stub's `num_series` sums to the same).
- Connectors shipped: `bdp_fetch` (observations), `enumerate_bdp` (hierarchy-crawl catalog feed), `bdp_search` (local catalog search).
- Completeness verdict: catalog covers ALL series? **YES** (the crawl yields exactly each dataset's declared `num_series`; spot-verified against the two stress cases — deepest pagination and most datasets — and self-checked at build time). Connectors cover ALL accessible data classes? **YES** — `datasets/{id}/` observations are the only value-bearing endpoint; everything else is metadata the catalog folds in.
- Known gaps / deliberate exclusions: the dimension-catalogue endpoints (`/domains/{id}/dimensions/…`) are reference metadata (the SDMX code-lists), not time series — not wrapped as fetch verbs; their signal already rides in the series descriptions.
- **Two defects fixed vs the previous (0.7) connector** — see §1.4 and §8: (1) the datasets list was never paginated, silently dropping every dataset past the first 10 in the 3 domains that have >10 (e.g. domain 19 has 25); (2) the per-dataset crawl paged at the default 10 series/page **with full observation history**, ~7,200 pages and 502-prone. Now: paginate the datasets list, and crawl at `page_size=100&obs_last_n=1` (~720 pages, tiny payloads).

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | BPstat Data API (ReDoc / drf-yasg) | https://bpstat.bportugal.pt/data/docs | portal | yes | JS SPA; the real spec is the OpenAPI below |
| 2 | **OpenAPI spec (drf-yasg)** | `https://bpstat.bportugal.pt/data/docs/?format=openapi` | spec | yes | machine-readable; the authoritative endpoint list (§1.3) |
| 3 | BPstat portal | https://bpstat.bportugal.pt | web | yes | human-facing browser the API mirrors |
| 4 | Homepage | https://www.bportugal.pt | web | yes | institution |

> The docs page is a drf-yasg + ReDoc single-page app — `WebFetch` only sees the title. The spec is served at `…/data/docs/?format=openapi` (also `…/data/swagger/?format=openapi`); both return the same 30 KB OpenAPI JSON. `/data/swagger.json` 404s — use the `?format=openapi` form.

### 1.2 The data model (provider's own terms)

- Atomic fetchable unit: a **series** (a set of observations sharing a fixed set of dimension categories except `reference_date`), identified by a numeric integer **series id** (e.g. `12099329`).
- Hierarchy: **domain (topic) → dataset → series**.
  - A **domain** is a topic node in a tree (`parent_id`); 77 total, **65 leaf** (`has_series=true`). Carries `num_series`, `num_datasets`, `obs_updated_at`.
  - A **dataset** is "the full set of series in a domain that share the same set of *dimensions*" (not categories). So a domain has one dataset per distinct dimension-set; most have 1, up to 25 (domain 19). Identified by a content-addressed hex id (e.g. `aea9d7f70ddf9c6de29feaeba86a9456`). Carries `num_series`, `obs_updated_at`.
  - A **series** carries `id`, `label`, `short_label`, a rich `description`, `obs_updated_at`, `dataset_id`, `domain_ids[]`, and a `dimension_category[]` list of `{dimension_id, category_id}` SDMX coordinates.
- Metadata travels **bilingually**: every label / description is served in `PT` (default) or `EN` via the `lang` query param.
- Frequencies: annual `A`, biannual `S`, quarterly `T`, monthly `M`, daily `D` (the `recurrence` filter vocabulary). Frequency is itself a dimension, so a single dataset can mix frequencies — there is no clean dataset-level frequency. The frequency word ("Monthly" / "Mensal") is spelled out inside each series' `description`.

### 1.3 Endpoint reference (live-verified, from the OpenAPI spec)

| Endpoint (under `/data/v1`) | Method | Required | Key optional params | Returns | Pagination |
|---|---|---|---|---|---|
| `/domains/` | GET | — | `lang` | list of all 77 domains | none |
| `/domains/{id}/` | GET | `domain_id` | `lang` | one domain (incl. `num_series`) | none |
| `/domains/{id}/datasets/` | GET | `domain_id` | `lang`, `page`, `page_size`, `dimension_ids` | JSON-stat collection; `link.item[]` = dataset stubs | `page`/`page_size`; `extension.next_page` cursor |
| `/domains/{id}/datasets/{dataset_id}/` | GET | `domain_id`, `dataset_id` | `series_ids`, `lang`, `obs_since`, `obs_to`, `obs_last_n`, `recurrence`, `dim_cats`, `decimal`, `page`, `page_size` | **JSON-stat 2.0 dataset** — observations + `extension.series[]` (id, label, dimension_category) | `page`/`page_size` (**max 100**); `extension.next_page` cursor |
| `/domains/{id}/dimensions/` | GET | `domain_id` | `lang`, `page`, `page_size` | dimensions available in the domain | paginated |
| `/domains/{id}/dimensions/{dim_id}/` | GET | both | `lang` | one dimension + its categories (JSON-stat) | none |
| `/series/` | GET | **`series_ids`** | `lang` | rich per-series metadata (label, short_label, **description**, dataset_id, domain_ids, dimension_category) — **no observations** | **max 100 ids/call** |

There is **no** bulk-download / export / FTP surface and **no** "list all series" endpoint (`/series/` *requires* `series_ids`).

### 1.4 The "full universe" question

- **Authoritative enumeration path:** there is no flat index — the universe is the union over the hierarchy. Walk `/domains/` → keep the 65 leaf domains → for each, **paginate** `/domains/{id}/datasets/` → for each dataset, **paginate** `/domains/{id}/datasets/{dataset}/` reading `extension.series[].id`. **Archetype B.**
- Pagination / recursion / fan-out? **Yes, on two levels** — the datasets list *and* the dataset detail both paginate. This is the crux of completeness, and where the previous connector failed:
  - **Datasets list (FIXED):** the old `_list_datasets` read only `link.item` on page 1 (default `page_size=10`). 3 domains have >10 datasets (domain 19 has 25), so it **silently dropped 15 datasets** there — and all their series. The catalog must page through `extension.next_page` (or pass `page_size=100`).
  - **Dataset detail (OPTIMISED):** `page_size` defaults to 10 and **caps at 100** (101+ → HTTP 400). The detail response also drags an observation `value[]` array; the old crawl pulled full history at 10 series/page → ~7,200 pages, 40 KB each, and 502s when a 100-series page tried to serialise full history. **`obs_last_n=1` cuts the value array to one point per series, which lets `page_size=100` succeed** (100 series, 101 values, ~70 KB). Net: ~720 pages instead of ~7,200, with tiny payloads.
- Estimated total: **72,063 series** — `sum(domain.num_series)` over the 65 leaf domains, which equals `sum(dataset.num_series)` over all 212 datasets. Each dataset stub *declares* its `num_series`, giving a built-in per-dataset completeness oracle (the crawl must yield exactly that many distinct ids).
- Things that exist but are NOT in the enumeration path (treasury-trap): none found. There is no bulk-only or login-gated surface; the `domain → dataset → series` walk is exhaustive by construction (every series belongs to exactly one dataset under its domain).
- Anything gated behind a higher plan / login: none — fully open.

---

## 2. Authentication & access

- Auth required? **No.** Keyless, open API. No header, no key, no `secrets=`.
- How to obtain a key: n/a.
- Free-tier limits / rate limits: none documented. BPstat sits behind **Akamai**; aggressive crawling gets throttled (the old full-history crawl is the failure mode). The connector throttles conservatively (concurrency 4, 0.25 s inter-request delay, browser User-Agent, retry on 403/429/5xx). The build cost is now ~720 lean pages + ~1,440 enrichment calls, well within polite limits.
- ⛔ Human intervention needed? **No** — nothing to register, no secret to add to `ockham/.env`.
- Secret handling plan: n/a (keyless). `bdp_fetch` takes no `api_key`; the error-mapping suite runs with `env_key=None`.

---

## 3. Transport & quirks

- Base URL: `https://bpstat.bportugal.pt/data/v1`.
- Response format: **JSON** throughout (JSON-stat 2.0 for dataset detail; plain JSON lists for `/domains/` and `/series/`). `fetch_json` handles `bdp_fetch`; the bulk crawl uses the shared `ThrottledJsonFetcher`.
- Pagination: `page`/`page_size` query params, plus an `extension.next_page` absolute-URL cursor on multi-page responses. `page_size` max = 100 on both list endpoints.
- Rate-limit headers: none surfaced; back off on 403/429/5xx (Akamai may answer 403 under load).
- Anti-bot / TLS-fingerprinting: Akamai-fronted but stock `httpx` works with a browser User-Agent + `Origin`/`Referer` headers; no `curl_cffi` needed.
- Date / number formats: reference dates are `YYYY-MM-DD` strings in the JSON-stat time dimension index; observation values are floats (or `null`). `decimal=true` would return fixed-point strings — not used.
- Landmines:
  - **`page_size` only works ≤100, and `page_size=100` alone 502s on a big dataset** unless paired with `obs_last_n` (or a small `obs_*` window) to shrink the value array.
  - **Two pagination levels** — forget the datasets-list one and you silently undercount (the old bug).
  - The JSON-stat `value[]` is a flat row-major `(series × dates)` array; melting it requires the time-dimension index length to stride correctly (see `_parse_dataset_observations`).

---

## 4. Catalog plan (as built)

- Strategy: **enumerator + catalog_build**, archetype **B** (paginated hierarchy crawl) + a **bilingual enrichment pass**.
- Namespace: `bdp`. Code scheme:
  - series → `"{domain_id}:{dataset_id}:{series_id}"` (compound — a series id is only meaningful with its dataset/domain for the fetch dispatch).
  - dataset stub → `"dataset:{domain_id}:{dataset_id}"`.
  - domain stub → `"domain:{domain_id}"`.
  Split entity types by the KEY prefix or the `entity_type` column.
- Entity shape: KEY=`code` (ns `bdp`), TITLE=`title` (EN label), METADATA=[`description` (folded EN + PT rich descriptions + domain/dataset context), `entity_type`, `domain_id`, `domain_name`, `dataset_id`, `dataset_label`, `title_pt` (PT label), `short_label`, `num_series`, `last_update`, `source`].
- **Enumeration → enrichment split:** the crawl's job is purely **ID discovery** (it yields `id` + EN `label` + the dataset's declared count for the self-check). The rich, search-bearing text comes from a separate **`/series/?series_ids=` enrichment** in batches of 100, in **EN and PT** — the same endpoint returns a far richer `description` than the crawl's terse `label` (e.g. *"Assets of insurance corporations and pension funds in loans of the financial sector - consolidated data - transactions for the year ending the quarter in millions of euros"* vs the label *"Assets of insurers and pensions in loans of the financial sector-accum transactions"*). EN description is the primary search signal; PT description folds in for Portuguese recall (the BM25 analogue of BdF's EN+FR fold). Best-effort, batched, retried, split-on-failure (the bde enrichment pattern).
- Index policy: `discovery_indexes()` — `code`=BM25, `title`/`description`=adaptive (BM25-only above 1000 unique → no multilingual embedding, hence the bilingual `description` text). `default_field="title"`.
- Multi-bundle? No — one `bdp` bundle (~72 K series + 212 dataset stubs + 65 domain stubs) is tractable.
- Catalog URL: `hf://parsimony-dev/bdp` · env override `PARSIMONY_BDP_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `bdp_search` | @connector (local search) | yes | discovery | the whole catalog |
| `bdp_fetch` | @connector | no | observations fetch | every series (the one value-bearing endpoint) |
| `enumerate_bdp` | @enumerator | no | catalog feed | the full 72 K-series universe + stubs |

- Endpoints deliberately NOT wrapped: the `/domains/{id}/dimensions/…` code-list endpoints (reference metadata, not time series). Their content (dimension/category names) is already embedded in each series' `description` prose.

## 6. Output schemas

- `bdp_fetch` → KEY `series_id` (param_key `dataset_id`, ns bdp) + TITLE `title` + DATA `date`(datetime), `value`(numeric). One row per (series, observation); `series_ids` filters, `start_date`/`end_date`/`lang` bound. Dispatch: `bdp_search` returns the compound `domain:dataset:series` code; the agent splits it and calls `bdp_fetch(domain_id, dataset_id, series_ids=series)`.
- `bdp_search` → KEY `code`(ns bdp) + TITLE `title` + DATA `score`.
- `enumerate_bdp` → KEY `code`(ns bdp) + TITLE `title` + 10 METADATA (see §4).

## 7. Tests

- `test_bdp_connectors.py` — offline respx: fetch (JSON-stat parse, multi-series melt, None-param drop, EmptyData, ParseError on non-dict, 404→ProviderError, invalid dataset_id/lang); enumerate bounded crawl via the `_list_domains` seam (exact schema, domain/dataset/series rows, EN+PT folded, dataset-list pagination followed, page-cap), empty-on-domains-failure.
- `test_error_mapping_bdp.py` — `ErrorMappingSuite` (keyless, `env_key=None`), route `domains/11/datasets/ABC/`.
- `test_integration_bdp.py` — live (keyless): bounded single-domain crawl via the `_list_domains` seam with a request counter asserting the bound; keyed fetch of the domain-48 dataset; fixture-catalog search.
- `test_build_catalog.py` — index types + `default_field`.
- `catalog_tests/queries.yaml` — recall gate (exact `code:` + distinctive `title_bm25` required; semantic `hybrid_title` optional).

---

## 8. Live verification log (documentation is a claim; execution is the truth)

> All rows are my own live executions on 2026-06-08 against the production BPstat API (keyless).

| Date | Check | Expected (docs) | Actual (live) | Verdict |
|------|-------|-----------------|---------------|---------|
| 06-08 | domain count + leaf count | — | `/domains/` = **77** total, **65** `has_series` | ✅ |
| 06-08 | universe size | "~72 K" (old docstring) | `sum(num_series)` over leaf domains = **72,063**, == `sum(dataset.num_series)` over 212 datasets | ✅ measured |
| 06-08 | flat "list all series" path exists? | — | **no** — `/series/` returns HTTP 400 `series_ids required`; no export endpoint | ✅ refuted |
| 06-08 | datasets-list pagination (completeness bug) | old code read page 1 only | domain 19 returns **10 of 25** datasets on page 1 with `extension.next_page`→ page 2; `page_size=100` → all 25 (4,094 series) | ⛔→✅ fixed |
| 06-08 | dataset-detail `page_size` cap | default 10 | works to **100**; 110+ → HTTP 400; bare `page_size=100` → **502** (full-history value array too big) | ✅ measured |
| 06-08 | `obs_last_n=1` shrinks payload | — | `page_size=100&obs_last_n=1` → **200 OK**, 100 series, 101 values, ~70 KB | ✅ measured |
| 06-08 | `/series/?series_ids=` richness + batch cap | metadata | rich bilingual `description`+`short_label`, **no observations**; **max 100 ids/call** (200 → 400) | ✅ measured |
| 06-08 | **completeness — most-datasets domain** | declared 4,094 | full crawl of **domain 19** (25 datasets, paginated) → **4,094 unique series == declared**, 0 mismatch, 57 detail pages | ✅ measured |
| 06-08 | **completeness — deepest-pagination dataset** | declared 16,644 | full crawl of **domain 1** (single 16,644-series dataset) → **16,644 unique == declared**, 0 mismatch, 167 detail pages | ✅ measured |
| 06-08 | per-dataset self-check holds | crawl == stub `num_series` | every dataset in domain 19 crawled to its exact declared count | ✅ measured |

**Completeness sign-off (2026-06-08):** the catalog contains **all 72,063 BdP series**.
The universe is exhaustively defined by the `domain → dataset → series` walk (every
series belongs to exactly one dataset under exactly one domain), each dataset
*declares* its `num_series`, and the crawl is verified to recover that exact count —
proven on the two stress cases that break naive crawlers: the **most-datasets**
domain (19: 25 datasets, where the old code dropped 15) recovered 4,094/4,094, and
the **deepest single dataset** (1: 16,644 series across 167 pages) recovered
16,644/16,644. The enumerator
also self-checks crawled-vs-declared per dataset at build time and logs any shortfall.
The previous connector under-counted (datasets-list never paginated) — now fixed. The
fetch verb covers the only value-bearing endpoint (`datasets/{id}/` observations);
the dimension code-list endpoints are reference metadata folded into the catalog, not
wrapped. Verified by: live spec compilation + universe measurement + the page_size /
obs_last_n optimisation + two stress-case completeness crawls + per-dataset self-check
+ live `bdp_fetch` + offline/live tests green + ruff/mypy/`parsimony list --strict`
clean. Signed: autonomous pass, 2026-06-08.

---

## 9. Open questions / follow-ups

- [x] **Deepest-pagination crawl (domain 1, 16,644 series) confirmed.** Live crawl recovered 16,644/16,644 unique series across 167 pages, 0 mismatch. Both stress cases (most-datasets + deepest-pagination) now pass exactly.
- [ ] **Build + publish the snapshot.** Enumeration completeness is proven; the remaining maintainer step is the heavy one-time build: full crawl + EN/PT enrichment of all 72 K series → embed → `validate_catalog.py` (expect `required_recall 1.00`) → push to `hf://parsimony-dev/bdp`. Not run in this pass (heavy enrichment fan-out + embed). Existing snapshot (if any) is stale/pre-fix.
- [ ] **Drop EN enrichment to halve build calls?** EN comes from both the crawl `label` and the `/series/` EN `description`; the latter is richer but the marginal recall gain over the label is modest. If the ~720 EN-enrichment calls become a throttling problem, fall back to crawl-label-only EN and keep PT enrichment.
- [ ] **Per-series structured `frequency` column.** Deliberately dropped (frequency is a dimension, so it's per-series not per-dataset, and lives in the prose `description` in both languages). Revisit only if `FIELD:value` frequency filtering is requested — it would need parsing each series' `dimension_category` against the domain's Periodicity dimension.
