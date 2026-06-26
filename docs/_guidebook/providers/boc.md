# Provider dossier: Bank of Canada (`boc`)

> One file per provider/system. This is the **single place** where ALL of the
> provider's documentation, API behaviour, and our findings are compiled before
> a line of connector code is written.
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** Bank of Canada (Banque du Canada)  ·  **Key:** `boc`  ·  **Homepage:** https://www.bankofcanada.ca
**Distribution:** `parsimony-boc`  ·  **Namespace(s):** `boc`
**Kind:** public-keyless
**Status:** ✅ verified-live  ·  **Owner:** connectors-sweep  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- What this provider serves, in one sentence: Canadian macro + financial time series
  (FX rates, interest/bond yields, money & credit aggregates, CPI, commodity price
  indices, and the data behind BoC publications) via the **Valet** JSON/CSV/XML API.
- Auth: **none** (keyless public API). `load(*, catalog_url=...)` binds only the catalog URL.
- Discovery model: **we build a catalog** (no native keyword search). Archetype **A** —
  one live full-index call (`/lists/series/json`) lists the entire series universe.
- Total addressable universe: **15,638 series + 2,441 groups** (live, 2026-06-09).
  Counted by `len(/lists/series/json .series)` and `len(/lists/groups/json .groups)`.
- Connectors shipped: `boc_fetch`, `enumerate_boc`, `boc_search`.
- Completeness verdict: catalog covers ALL? **YES** (Q1 proven — see §8) · connectors cover
  ALL? **YES** (Q2 — ~99.7% of listed series fetchable; the rest fail with a clean
  `EmptyDataError`; every group panel fetchable).
- Known gaps / deliberate exclusions: ~29 **retired groups** (1.2%) that 404 on every fetch
  path are pruned from the catalog via the membership fan-out's liveness signal; a thin tail of
  stale series (~0.3%) carry no observations and return `EmptyDataError`. The observations
  endpoint caps the **request URL at ~4096 bytes** (≈100–160 series depending on name length),
  guarded pre-network.

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Last seen current? | Notes |
|---|--------|-----|------|--------------------|-------|
| 1 | Valet API docs | https://www.bankofcanada.ca/valet/docs | api-docs | 2026-06-09 | a **JS SPA** — `WebFetch` sees only the `<title>`; the authoritative reference is live probing (§8) |
| 2 | Valet docs (terms-of-use endpoint) | every response carries `terms.url` | api | 2026-06-09 | → https://www.bankofcanada.ca/terms/ |
| 3 | Content reuse terms | https://www.bankofcanada.ca/terms/ | legal | 2026-06-09 | free reuse **with attribution**; derived index of ids+titles permitted (§2) |
| 4 | Series list (the full index) | https://www.bankofcanada.ca/valet/lists/series/json | bulk | 2026-06-09 | 15,638 series; **the authoritative enumeration path** |
| 5 | Group list | https://www.bankofcanada.ca/valet/lists/groups/json | bulk | 2026-06-09 | 2,441 groups (named panels) |

The Valet docs page renders client-side, so this dossier's endpoint reference (§1.3) is built
from **live probing**, which is in any case the source of truth (the completeness mandate).

### 1.2 The data model (in BoC's own terms)

- Atomic fetchable unit: a **series** (a single observation stream, e.g. `FXUSDCAD` =
  USD/CAD daily noon-ish rate; `V39079` = GoC 10-year benchmark bond yield).
- Identity: an opaque alphanumeric series **name** (`FXUSDCAD`, `V39079`, `A.AGRI`,
  `CES_C11G_SELLING_R`). No global numbering scheme; names mix several legacy conventions.
- Hierarchy above it: **groups** — named panels that bundle related series
  (`FX_RATES_DAILY` holds 27 currency pairs; `A4_FUNDS_CONSUMER` holds consumer-credit
  aggregates). A series may belong to several groups; most belong to exactly one. Groups are
  *not* a strict partition and are addressable in their own right.
- Metadata per unit: series carry `label` (title) + `description`; groups carry `label` +
  `description` (the group description is often the only place units/frequency live, e.g.
  "Month-end, Millions of dollars").
- Frequencies: daily / weekly / monthly / quarterly / annual — not declared as a field in the
  list endpoints; it rides the prose `description`.

### 1.3 Endpoint reference (verbatim from live behaviour)

Base URL: `https://www.bankofcanada.ca/valet`. Formats: `/json`, `/csv`, `/xml` (we use JSON).

| Endpoint | Method | Required | Optional | Returns | Notes |
|----------|--------|----------|----------|---------|-------|
| `/lists/series/json` | GET | — | — | `{terms, series:{name:{label,description,link}}}` | the full series index (15,638) |
| `/lists/groups/json` | GET | — | — | `{terms, groups:{name:{label,description,link}}}` | the full group index (2,441) |
| `/groups/{name}/json` | GET | name | — | `{terms, groupDetails:{name,label,description,groupSeries:{name:{label,link}}}}` | a group's membership; **404 for retired groups** |
| `/series/{name}/json` | GET | name | — | `{terms, seriesDetails:{…}}` | series detail (note: key is `seriesDetails`, **plural**, ≠ the observations payload's `seriesDetail`) — not used by the connector |
| `/observations/{names}/json` | GET | names (comma-joined) | `start_date`,`end_date`,`recent`,`recent_weeks`,`recent_months`,`recent_years` | `{terms, seriesDetail:{name:{label,description}}, observations:[{d, name:{v}}]}` | the fetch path; **request URL capped ~4096 bytes** |
| `/observations/group/{name}/json` | GET | name | same date params | same shape (whole panel) | one group's full panel in one call; 404 for retired groups |

Date filtering: `start_date`/`end_date` (`YYYY-MM-DD`) or one of the `recent*` shortcuts.
Missing/suppressed observations come back as `{"v": ""}` (or the key absent), not an error row.

### 1.4 The "full universe" question

- **Authoritative enumeration path: `GET /lists/series/json`** — a single call that returns
  every addressable series. Archetype **A** (live full-index). The catalog self-tracks BoC
  additions and `len(catalog series rows)` diffs directly against this endpoint's count.
- Pagination / recursion: **none** for the series index (one ~3.5 MB JSON object). The
  series→group annotation requires a per-group fan-out (`/groups/{name}/json` × 2,441), but
  that is *enrichment*, not the enumeration of record.
- Estimated total: **15,638 series**, **2,441 groups** (live 2026-06-09). BoC does not publish
  a headline count; these are measured directly.
- Things that exist but are NOT in `/lists/series`: **nothing found.** The 2,441-group
  membership fan-out surfaces 15,279 distinct member series and **0** that are absent from
  `/lists/series` — i.e. the master list is a true superset of every group's membership (§8).
  This is the completeness proof: there is no series reachable via a group but missing from the
  index.
- Anything gated behind a higher plan / login: none — fully open.

---

## 2. Authentication & access

- Auth required? **No.** Keyless public API. No `secrets=`, no `bind(api_key=…)`, no
  `UnauthorizedError` on the data path. `load(*, catalog_url=None)` binds only the search
  catalog URL.
- Rate limits: none documented; the API tolerates the connector's group fan-out at concurrency
  16 (2,441 requests in ~69 s, 2026-06-09) without throttling. Best-effort per-group failures
  are swallowed (retired groups 404).
- Terms of use (https://www.bankofcanada.ca/terms/): free reuse **provided you attribute the
  Bank of Canada as the source and indicate if changes were made**, and exercise due diligence
  on accuracy. A derived catalog of series ids + titles is within the permissive grant. (Bank
  note images / logos / wordmarks are excluded — we ship none.) README + snapshot carry BoC
  attribution.

---

## 3. Transport & quirks

- Base URL: `https://www.bankofcanada.ca/valet`.
- Response format: JSON (`fetch_json` fits cleanly — GET + `raise_for_status` + typed-error
  mapping). CSV/XML also offered but unused.
- Pagination: none on the list endpoints; the series index is one object.
- **Chunked-transfer close (cosmetic):** raw `curl` on `/lists/series/json` prints
  `curl: (18) transfer closed with outstanding read data remaining`, but the body is complete
  and valid JSON. `httpx` (the kernel transport) reads all 15,638 series and 3,580,716 chars
  with no error — the warning is curl's pickiness about the connection close, not data loss.
- **Observations request-URI cap ≈ 4096 bytes.** `/observations/{names}/json` redirects (HTTP
  **302**, to an error page) once the request URL exceeds ~4 KB. Measured boundary: full URL
  4087 bytes → 200, 4127 bytes → 302 (path 4051 → ok, 4091 → 302). It is **URL-length-bound,
  not series-count-bound** — 140 short names (876 chars) pass, 140 long names (5,809 chars)
  302. Guard the assembled URL pre-network (`InvalidParameterError` with split/`group:`
  guidance) rather than letting an agent hit an opaque 302 → `ParseError`.
- Date/number formats: ISO dates; values are decimal strings (coerce to float; suppressed
  values are `""` → `None`). No locale traps.
- Other landmines: ~29 **retired groups** appear in `/lists/groups/json` but 404 on both
  `/groups/{name}/json` and `/observations/group/{name}/json` (e.g. `EXP_20220303`,
  `FSR_2018JUNE` — one-off panels for dated publications). Don't catalog them as live.

---

## 4. Catalog plan

- Strategy: **enumerator + catalog_build** (archetype A). One `enumerate_boc` emits one row per
  series **and** one row per (live) group; `build_boc_catalog` indexes them; `boc_search`
  queries the published snapshot.
- Namespace: `boc` (single namespace; series and `group:`-prefixed group rows coexist).
- Code scheme: a series row's KEY is its bare name (`FXUSDCAD`); a group row's KEY is
  `group:{NAME}` (`group:FX_RATES_DAILY`) — the exact string `boc_fetch` accepts, so a search
  hit routes straight to a fetch with no transformation.
- Entity shape: KEY=`series_name` (ns `boc`), TITLE=`title`,
  METADATA=[`description`,`source`,`entity_type`,`group`,`group_label`].
  `entity_type ∈ {series, group}` lets agents weight/filter by granularity; `group`/
  `group_label` annotate a series with its panel (97.7% coverage).
- Enumeration code: `/lists/series/json` (series rows) + `/lists/groups/json` (group rows) +
  a concurrency-capped `/groups/{name}/json` fan-out for series→group membership **and** group
  liveness (404 ⇒ prune the group row).
- Index policy: `discovery_indexes()` — `code`→BM25, `title`/`description`→adaptive (BM25-only
  here: >1000 unique values). `default_field="title"`.
- Multi-bundle? No — one flat `boc` catalog (~18k rows fits comfortably).
- Catalog URL: `hf://parsimony-dev/boc` · env override `PARSIMONY_BOC_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers |
|-----------|-----------|-------|---------|--------|
| `boc_search` | @connector (make_local_search_connector) | yes | discovery | the whole catalog (series + group panels) |
| `boc_fetch` | @connector | no | fetch observations | any series by name (URL-bounded) or any group panel via `group:NAME` |
| `enumerate_boc` | @enumerator | no | catalog feed | the whole universe (15,638 series + live groups) |

- Endpoints deliberately NOT wrapped: `/series/{name}` (series detail) — its `label`/
  `description` already ride the list endpoints, so it adds no unique data. CSV/XML formats —
  JSON suffices. The `/observations/FX_RSS` feed — superseded by the `group:FX_RATES_DAILY`
  panel.

## 6. Output schemas

- Fetch: KEY=`series_name` (ns `boc`, `param_key="series_name"`), TITLE=`title`,
  DATA=`date`(datetime)+`value`(numeric).
- Enumerate: KEY=`series_name` (ns `boc`), TITLE=`title`,
  METADATA=`description`/`source`/`entity_type`/`group`/`group_label`.
- Search: `code`(KEY ns `boc`)/`title`(TITLE)/`score`(DATA). A `group:`-prefixed `code`
  routes to `boc_fetch(series_name="group:…")`; a bare `code` routes to
  `boc_fetch(series_name="…")`.

---

## 7. Tests

- `ErrorMappingSuite`: `boc_fetch`, route `…/observations/FXUSDCAD/json`, `env_key=None`
  (keyless — the canary-key leak check is vacuous but the status table still applies).
- Integration (live, env-gated): `boc_fetch` (FXUSDCAD, multi-series, group panel), bounded
  `enumerate_boc` (monkeypatched `_list_groups` → 2 groups + request counter), `boc_search`
  over a fixture catalog in `tmp_path`.
- Conformance: `assert_plugin_valid(parsimony_boc)`.
- Catalog probes `catalog_tests/queries.yaml`: `code:` probes (FXUSDCAD, group:FX_RATES_DAILY)
  + short `title_bm25` probes (exchange rate, bond yield).

---

## 8. Live verification log (documentation is a claim; execution is the truth)

| Date | Check | Expected | Actual (live) | Verdict | Action |
|------|-------|----------|---------------|---------|--------|
| 2026-06-09 | series index count | "~15.6k" | **15,638** (`/lists/series/json`) | ✅ | catalog target |
| 2026-06-09 | group index count | "~2.4k" | **2,441** (`/lists/groups/json`) | ✅ | group rows |
| 2026-06-09 | **Q1: any group member absent from `/lists/series`?** | none | fan-out over all 2,441 groups → 15,279 distinct members, **0 outside master** | ✅ **complete** | `/lists/series` is authoritative |
| 2026-06-09 | series→group coverage | — | 15,279 / 15,638 = **97.7%** annotated; 359 orphans (in no group) | ✅ | fan-out kept (earns its keep) |
| 2026-06-09 | retired groups (404 on detail) | — | **29 / 2,441** 404 on both `/groups/{name}` and `/observations/group/{name}` | ✅ | prune via fan-out liveness signal |
| 2026-06-09 | **Q2: are listed series fetchable?** | all | sample 300 → 299 return data (198 recent + 101 historical-only over full history); **1 stale** (`CES_C11G_SELLING_R`, 200+`observations:[]`) | ✅ ~99.7% | stale → clean `EmptyDataError` |
| 2026-06-09 | sample fetch parses real values | numeric | `FXUSDCAD` 2024Q1 → floats in (1.0,2.0), >1 distinct, real dates | ✅ | — |
| 2026-06-09 | **observations URL cap** | undocumented | **302 redirect above ~4096-byte request URL** (4087 ok → 4127 fail); URL-length-bound, not count-bound | ✅ | pre-network `InvalidParameterError` guard |
| 2026-06-09 | httpx vs chunked-close | full body | curl warns (18); httpx reads all 15,638 (3,580,716 chars), no error | ✅ | no robustness fix needed |
| 2026-06-09 | error mapping: list endpoint 5xx | typed | 503 → `ProviderError` (not raw httpx) | ✅ | — |

**Completeness sign-off:** the catalog contains ALL 15,638 addressable series plus every live
group panel — verified by diffing the group fan-out's 15,279 members against the master index
(0 outside) and sampling fetchability (299/300). The connectors expose every accessible data
class: any series by name (within the ~4 KB URL bound, which is guarded), any group panel by
`group:NAME`. Deliberate exclusions (series detail, CSV/XML, FX_RSS, the 29 dead groups, ~0.3%
stale series) are documented in §5 and §0. Signed: connectors-sweep on 2026-06-09.

---

## 9. Open questions / follow-ups

- [ ] **Publish the catalog snapshot** (build → `validate_catalog` → push `hf://parsimony-dev/boc`).
      A snapshot exists from the 0.5 era but predates the dead-group pruning + URL guard refactor.
      Deferred (maintainer step; needs `HF_TOKEN`) — not yet run.
- [ ] The 359 orphan series (in no group) are fetchable but have no panel; fine as-is. If BoC
      ever exposes a frequency field on the list endpoint, fold it into a METADATA column.
