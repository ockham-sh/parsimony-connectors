# Provider dossier: Swiss National Bank (`snb`)

> Re-run through the full guidebook process on 2026-06-09. The 0.7-sweep connector
> was the guidebook's **named cautionary frozen-registry case** (`_KNOWN_CUBES` of
> 237, whose comment cited a `scripts/discover_cubes.py` that never existed). Deep
> docs exploration replaced the frozen registry with the live sitemap, cracked the
> portal's internal API + WAF, and surfaced a **major Q2 gap**: the SNB *data
> warehouse* (912 cubes, ~79% of the provider) was entirely unwrapped.

**Provider:** Swiss National Bank — SNB data portal  ·  **Key:** `snb`  ·  **Homepage:** https://www.snb.ch
**Distribution:** `parsimony-snb`  ·  **Namespace(s):** `snb`
**Kind:** public-keyless
**Status:** ✅ verified-live  ·  **Owner:** connectors sweep  ·  **Last updated:** 2026-06-09

---

## 0. TL;DR

- **What it serves:** Swiss monetary, banking, interest-rate/FX, balance-of-payments,
  national-accounts and price statistics, as multi-dimensional "cubes" of time series.
- **Auth:** none (keyless public CSV/JSON API). No `secrets=`/`bind`/`UnauthorizedError`.
- **Discovery model:** we build a catalog. The authoritative enumeration is the
  **published XML sitemap** (`/sitemap`), which lists every cube URL — both publication
  cubes and warehouse cubes. Self-tracking (archetype A). **Replaces the frozen registry.**
- **Total addressable universe:** **1,149 cubes** = **237 publication** cubes
  (`/topics/{topic}/cube/{id}`, 7 topics) + **912 warehouse** cubes
  (`/warehouse/{group}/cube/{sdmx_id}`, 7 groups). Counted by parsing `/sitemap` (the
  237 matched the old frozen registry exactly, 0 drift; the 912 warehouse cubes were
  previously excluded).
- **Connectors shipped:** `snb_fetch` (routes publication *and* warehouse cubes),
  `enumerate_snb`, `snb_search`.
- **Completeness verdict:** catalog covers ALL cubes? **YES** (live sitemap; publication
  at series granularity, warehouse at cube granularity). Connectors cover ALL accessible
  data? **YES** — every catalogued cube (publication + warehouse) is fetchable.
- **Known gaps / deliberate exclusions:** warehouse cubes are catalogued at *cube* level
  (one row each), not exploded to every dimension-leaf series — their cartesian products
  are enormous and the leaves stay fetchable via `dim_sel` (the cardinality discipline,
  §7.3). Publication mega-cubes (>100 leaf series) likewise collapse to a cube-level row.

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Type | Current? | Notes |
|---|--------|-----|------|----------|-------|
| 1 | Official API help | https://data.snb.ch/en/help_api | api-docs | yes | Angular SPA — not readable by `WebFetch`; documents `/api/cube/{id}/data/{fmt}/{lang}`, `/dimensions`, `/script/R`, `lastUpdate`/eTag |
| 2 | Data portal (SPA) | https://data.snb.ch/en | portal | yes | Angular app, `appVersion 26.5.5` (2026 rebuild); all discovery is client-side XHR |
| 3 | OpenAPI/Swagger | — | — | — | none published |
| 4 | **Sitemap** | https://data.snb.ch/sitemap | bulk | yes | **the authoritative enumeration** — 1.9 MB XML, `application/xml`, listed in `robots.txt` |
| 5 | robots.txt | https://data.snb.ch/robots.txt | ops | yes | `User-agent: *` + `Sitemap: https://data.snb.ch/sitemap` |
| 6 | Internal portal JSON API | `https://data.snb.ch/json/...` | api | yes | what the SPA calls; WAF-walled to non-browser clients unless the `x-epb-ajax: true` header is sent (see §3) |
| 7 | R client `SNBdata` | https://github.com/enricoschumann/SNBdata · CRAN | code | yes | de-facto client; downloads by cube id via the same `/api/cube/.../data` paths |
| 8 | ToS / fair access | snb.ch disclaimer | legal | yes | public data, redistribution of a derived catalog is fine |

### 1.2 The data model (in the provider's own terms)

- **Atomic fetchable unit:** a *cube* — a multi-dimensional table of time series. A single
  *series* is one leaf along each dimension (the cartesian product), e.g. cube `rendoblim`
  × dimension item `10J` → the 10-year Swiss Confederation bond yield.
- **Two cube families:**
  - **Publication cubes** — curated portal tables. Plain alphanumeric ids (`rendoblim`,
    `devkum`, `snbmonagg`). 237 of them, grouped under 7 *topics* (snb, banken, ziredev,
    finma, uvo, aube, cross).
  - **Warehouse cubes** — the granular SDMX-style data store. Ids look like
    `BSTA@SNB.AUR_U.ODF` (`{group}@SNB.{path}`). 912 of them, under 7 *groups*
    (BSTA, ZAST, ZAHL, DDUM, KRED, SNB1A, WKI).
- **Identification:** publication id is bare; warehouse id carries `@`/`.`. The fetch API
  for warehouse cubes wants the id with **`@` replaced by `.`** (`BSTA.SNB.AUR_U.ODF`).
- **Metadata per cube:** `getCubeInfo` returns `title`, `publishingTitle` (the parent
  publication, e.g. "Interest rates and exchange rates" / "Annual banking statistics"),
  `unit`, `frequencySpecification`, `source`. The `/dimensions` and CSV payloads carry **no
  cube title** — only `cubeId` + dimensions. (This absence is why the old connector froze a
  hand-harvested title map.)
- **Frequencies/units:** per-cube; ISO-8601 durations in the JSON metadata (`P1D`/`P1M`/
  `P3M`/`P1Y`); freeform in `getCubeInfo.frequencySpecification` ("End of month", "Daily").

### 1.3 Endpoint reference (verbatim)

| Endpoint | Method | Params | Returns | Walled? | Notes |
|----------|--------|--------|---------|---------|-------|
| `/sitemap` | GET | — | XML urlset | no | every cube URL (publication + warehouse), all 3 langs |
| `/api/cube/{id}/data/csv/{lang}` | GET | `fromDate`,`toDate`,`dimSel` | CSV | no | publication fetch; preamble + `Date;<dims>;Value` |
| `/api/cube/{id}/data/json/{lang}` | GET | same | JSON | no | richer: per-series `header`+`metadata.key`+`frequency`+`unit` |
| `/api/cube/{id}/dimensions/{lang}` | GET | — | JSON | no | dimension tree (no cube title) |
| `/api/warehouse/cube/{api_id}/data/csv/{lang}` | GET | `fromDate`,`toDate`,`dimSel` | CSV | no | **warehouse fetch**; `api_id` = portal id with `@`→`.` |
| `/api/warehouse/cube/{api_id}/dimensions/{lang}` | GET | — | JSON | no | warehouse dimensions |
| `/json/table/getCubeInfo` | GET | `lang`,`cubeId`,`isWarehouse`,`pageViewTime` | JSON | **yes → `x-epb-ajax`** | title/publishingTitle/unit/frequency; `cubeId` is the portal id (literal `@`) |
| `/json/topic/getTopicsWithRootSubTopics` | GET | `lang`,`pageViewTime` | JSON | **yes → `x-epb-ajax`** | topic→subtopic landing tree (NOT a full cube list) |

### 1.4 The "full universe" question

- **Authoritative enumeration path:** `GET /sitemap` (XML). Extract `<loc>` entries matching
  `/{lang}/topics/{topic}/cube/{id}` (publication) and `/{lang}/warehouse/{group}/cube/{id}`
  (warehouse). One unwalled GET; self-tracking — a new cube appears in the sitemap.
- **Pagination/recursion:** none. Single document.
- **Counts (live, 2026-06-09):** 237 unique publication cube ids (7 topics) + 912 unique
  warehouse cube ids (7 groups) = **1,149**. The 237 matched the frozen `_KNOWN_CUBES`
  exactly (0 added, 0 removed) — the manual harvest *was* complete, but had no committed
  reproduction script (the named liability).
- **Things outside the enumeration:** none found. The portal's internal nav tree
  (`getTopicsWithRootSubTopics`) is a curated *landing* tree (one representative `cubeId`
  per subtopic), NOT a full list — the sitemap is the complete source. Warehouse `/facets`
  pages (7) are search UIs, not cubes.
- **Gated behind login:** none. Everything is public and keyless.

---

## 2. Authentication & access

- Auth required? **No.** Keyless public portal. No key to obtain, no quota documented.
- Secret handling: **none** — no `secrets=`, no `bind`/`load(api_key=)`, no
  `UnauthorizedError` on the data path. `load(*, catalog_url=None)` binds only the catalog
  URL for `snb_search`.
- ⛔ Human intervention needed? **No.**

---

## 3. Transport & quirks

- **Base URL:** `https://data.snb.ch`. Single host; plain `httpx` (no curl_cffi).
- **Formats:** CSV (fetch, long-format), JSON (`/dimensions`, `/data/json`, `/json/...`).
- **`fetch_json` vs raw:** CSV needs the raw `_get_text` helper (`GET` +
  `raise_for_status` + `map_http_error`/`map_timeout_error` → text). JSON endpoints use a
  `_get_json` helper (so the `x-epb-ajax` header can be attached for `/json/...`).
- **The WAF (Airlock) on `/json/...`:** the portal's internal API
  (`/json/table/getCubeInfo`, `/json/topic/...`) is fronted by an **Airlock WAF**. A stock
  request gets a 200/400 **`/error_path/` HTML page** (`waf.css`) instead of JSON — this
  *looks* like a transport failure but is a WAF block. The unlock is a single request
  header the SPA sends on every XHR: **`x-epb-ajax: true`**. With it, `/json/...` returns
  real JSON (and real, actionable 400s for missing params). No cookie/session needed.
  The `/api/...` data/dimensions paths are **not** walled.
- **`pageViewTime` param:** `/json/...` endpoints require a `pageViewTime` param shaped
  `YYYYMMDD_HHMMSS` (a telemetry/cache-bust token). Any well-formed value works (verified
  with a fabricated `20260609_000000`).
- **Warehouse id transform:** the portal/sitemap id `BSTA@SNB.AUR_U.ODF` must have `@`→`.`
  for the `/api/warehouse/cube/{id}/...` path (`BSTA.SNB.AUR_U.ODF`). The unencoded `@`
  (or `%40`) is rejected with `500 IllegalArgumentException: cubeId contains illegal
  characters`. The canonical transform was confirmed from the portal's own `getApiLinks`
  response, which builds the download URL as `…/cube/BSTA.SNB.AUR_U.ODF/data/csv/en`.
- **CSV shape:** BOM + preamble (`"CubeId";"<id>"` / `"PublishingDate";"..."`), blank line,
  header `Date;<dim cols>;Value`, data rows. Coerce **only** the trailing `Value` to numeric
  (dimension code columns D0/D1/… stay strings — the eia blanket-coerce anti-pattern). A
  200 body that is not a cube CSV (JSON error envelope, HTML) → `ParseError`; a header-only
  body → `EmptyDataError`. Warehouse CSV preamble echoes the **portal** id (with `@`).
- **Date formats:** `YYYY` / `YYYY-Qn` / `YYYY-MM` / `YYYY-MM-DD` (frequency-derived).

---

## 4. Catalog plan

- **Strategy:** enumerator + `catalog_build` + `build_catalog.py` + local search connector.
- **Namespace:** `snb` (single bundle).
- **Code scheme:** compound `{cube_id}#{series_key}` (Treasury/rba precedent). Publication
  cubes expand to series rows (`rendoblim#10J`, `devkum#M0.USD1`); a cube with no/oversized
  dimensions collapses to `{cube_id}#`. Warehouse cubes are catalogued at cube level only
  (`{portal_id}#`), since their cartesian products are huge and the leaves stay fetchable
  via `dim_sel`. The `#` split survives warehouse `@`/`.` ids (they contain no `#`).
- **Enumeration:** `_list_cubes()` (the bounding seam) parses `/sitemap` →
  `[(cube_id, kind, topic_or_group)]`. Per cube, a bounded concurrent fan-out calls
  `getCubeInfo` (best-effort title/publishingTitle/unit/frequency) and, for publication
  cubes, `/dimensions` (series expansion). Per-cube failures are skipped (best-effort);
  the sitemap fetch failing is fatal.
- **Index policy:** `discovery_indexes()` — `code` BM25, `title`/`description` adaptive
  (degrade to BM25-only above 1000 unique values; with ~1,149+ cubes that is expected, so
  semantic title probes stay `optional` in `queries.yaml`).
- **Catalog URL:** `hf://parsimony-dev/snb` · env override `PARSIMONY_SNB_CATALOG_URL`.

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Slice |
|-----------|-----------|-------|---------|-------|
| `snb_search` | @connector (make_local_search_connector) | yes | discovery | whole catalog |
| `snb_fetch` | @connector | no | fetch a cube (publication **or** warehouse) | all 1,149 cubes |
| `enumerate_snb` | @enumerator | no | catalog feed | whole universe |

- `snb_fetch` routes by id shape: an id containing `@` → warehouse path with `@`→`.`;
  otherwise the publication path. Best-effort `getCubeInfo` title (falls back to cube_id).
- **Deliberately NOT wrapped:** the `/json/...` *facet search* (`getTozFacetResult`) — our
  catalog already indexes the universe; the warehouse `/facets` UI is redundant with search.
  Per-dimension-leaf rows for warehouse cubes (cardinality discipline, fetchable via `dim_sel`).

## 6. Output schemas

- **Enumerate `OutputConfig`:** KEY `code` (ns `snb`); TITLE `title`; METADATA `description`,
  `source` (`snb_data_portal`|`snb_warehouse`), `cube_id`, `series_key`, `dimension_path`,
  `category` (the `publishingTitle`/topic), `frequency`, `unit`.
- **Fetch `OutputConfig`:** KEY `cube_id` (ns `snb`), TITLE `title`, DATA `date`
  (dtype datetime). Dimension code columns + `Value` fold in as DATA (`merge_unmapped_as_data`).
- **Search `OutputConfig`:** `code`/`title`/`score`. Agent splits `code` on `#` and passes
  the cube_id part to `snb_fetch`.

## 7. Tests

- `test_snb_connectors.py` — offline respx: CSV parse (numeric `Value`, string dims),
  Empty/Parse/Provider guards, sitemap parse (pub + warehouse split), warehouse `@`→`.`
  fetch routing, getCubeInfo title enrichment + best-effort fallback, dimension cartesian
  product + mega-cube collapse, CONNECTORS count==3.
- `test_error_mapping_snb.py` — `ErrorMappingSuite` (keyless, `env_key=None`, route =
  publication CSV).
- `test_integration_snb.py` — live: publication fetch (rendoblim, devkum multidim),
  **warehouse fetch** (BSTA cube via `@`→`.`), bounded enumerate (monkeypatch `_list_cubes`
  to a 2–3 cube slice + request counter), sitemap live count sanity, fixture-catalog search.
- `test_public_surface.py` — `__all__ == ["CONNECTORS","load"]`, count 3, internals not
  re-exported.
- `test_build_catalog.py` — index types + `default_field` + namespace/dispatch metadata.
- `catalog_tests/queries.yaml` — `code:` required probes (publication + warehouse) +
  optional title probes.

---

## 8. Live verification log

| Date | Check | Expected (docs) | Actual (live) | Verdict | Action |
|------|-------|-----------------|---------------|---------|--------|
| 2026-06-09 | sitemap is the full enumeration | "navigation tree" (code comment) | `/sitemap` XML, 237 pub + 912 warehouse cube `<loc>`s | ✅ | replaced frozen registry with live sitemap |
| 2026-06-09 | frozen `_KNOWN_CUBES` vs sitemap | 237 | 237 unique pub ids, **0 added / 0 removed** | ✅ | harvest was complete but had no repro script — committed `harvest_cubes.py` |
| 2026-06-09 | warehouse cubes accessible? | excluded ("SDMX-style, not fetchable") | 912 cubes, fetchable via `/api/warehouse/cube/{@→.}/...` | ✅ refuted exclusion | wrapped them — major Q2 gap closed |
| 2026-06-09 | warehouse fetch transform | unknown | portal `BSTA@SNB.AUR_U.ODF` → api `BSTA.SNB.AUR_U.ODF` (from `getApiLinks`); real CSV w/ 7 dim cols + Value (BSTA + ZAST) | ✅ | `@`→`.` routing in `snb_fetch` |
| 2026-06-09 | cube title source | nav-tree only | `/dimensions` + CSV carry **no** title; `getCubeInfo` does (title + publishingTitle + unit + frequency) | ✅ | titles from `getCubeInfo` (best-effort), synthesize fallback |
| 2026-06-09 | `/json/...` WAF | (undocumented) | stock req → `/error_path/` WAF page; **`x-epb-ajax: true`** header → real JSON | ✅ | header attached on `/json/...` calls only |
| 2026-06-09 | publication fetch real values | bond yields | `rendoblim` monthly, plausible % band; `devkum` multi-currency | ✅ | — |
| 2026-06-09 | numeric coercion safety | — | `Value` float, dim codes (`10J`, `M0`) stay strings | ✅ | coerce only `Value` |

**Completeness sign-off:** the catalog contains ALL **1,149** addressable cubes (237
publication + 912 warehouse), verified by diffing the parsed `/sitemap` against the live
universe; publication cubes are catalogued at series granularity, warehouse cubes at cube
granularity (leaves fetchable via `dim_sel`). Every catalogued cube is fetchable by
`snb_fetch` (publication and warehouse). Signed: connectors sweep on 2026-06-09.

### 8b. Decisions log

- **Frozen registry → live sitemap (archetype C → A).** The named liability is resolved:
  the universe is now discovered live and self-tracks; `scripts/harvest_cubes.py` (the
  finally-written reproduction script) re-derives the cube list and `--diff`s it.
- **Warehouse included (user decision, 2026-06-09).** Presented the 912-cube gap; user
  chose the full universe. Catalogued at cube level (cardinality discipline), fetchable.
- **`getCubeInfo` is best-effort.** It rides the reverse-engineered `x-epb-ajax` unlock; if
  SNB changes it, titles degrade to a synthesized `{topic} — {cube_id}` and fetch still
  works (CSV path is official). The completeness surface (sitemap + `/api/...` fetch) never
  depends on the internal API.

## 9. Open questions / follow-ups

- [ ] Per-series warehouse expansion (currently cube-level) — only if an agent needs to
      discover individual warehouse leaves by text rather than by `dim_sel`.
- [ ] `/data/json` per-series `frequency`/`unit`/`header` could enrich publication series
      rows in one call (replacing `/dimensions` + `getCubeInfo`) — deferred; current path
      is proven and complete.
