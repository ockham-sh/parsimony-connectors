# Provider dossier: Sveriges Riksbank (`riksbank`)

**Provider:** Sveriges Riksbank (Sweden's central bank)  ·  **Key:** `riksbank`  ·  **Homepage:** https://www.riksbank.se
**Distribution:** `parsimony-riksbank`  ·  **Namespace(s):** `riksbank`
**Kind:** public-keyless (optional quota-raising key)
**Status:** ✅ verified-live  ·  **Owner:** autonomous  ·  **Last updated:** 2026-06-10

---

## 0. TL;DR

- **Serves:** Sweden's central-bank statistics across **five** public REST APIs — interest &
  exchange rates (SWEA), the Swedish Krona Short-Term Rate (SWESTR), Monetary Policy
  forecasts & outcomes, market Turnover Statistics, and securities Holdings.
- **Auth:** none. Optional `Ocp-Apim-Subscription-Key` header (env `RIKSBANK_API_KEY`) only
  raises the keyless quota (**5 req/min, 1000/day per IP**).
- **Discovery model:** we build a catalog. Two products self-enumerate live (SWEA `/Series`,
  Monetary Policy `/forecasts/series_ids`); the other three have small stable dimensions →
  static registries.
- **Total addressable universe: 156 catalog units** — 117 SWEA + 7 SWESTR + 24 Monetary
  Policy + 6 Turnover (market×freq) + 2 Holdings. (Live-counted 2026-06-10; 0 duplicate codes.)
- **Connectors shipped:** `riksbank_fetch`, `riksbank_swestr_fetch`,
  `riksbank_monetary_policy_fetch`, `riksbank_turnover_fetch`, `riksbank_holdings_fetch`,
  `enumerate_riksbank`, `riksbank_search` (7).
- **Completeness verdict:** catalog covers ALL? **YES**. connectors cover ALL? **YES**.
- **Known gaps / deliberate exclusions:** SWESTR `PRESWESTR` (preliminary 2021 test data);
  SWEA derived endpoints (`CrossRates`/`ObservationAggregates`/`CalendarDays` — computed
  conveniences over the same 117 series, no new units); Turnover Excel-report endpoints
  (duplicate the JSON).

> **The headline finding.** The prior connector covered only **2 of the Riksbank's 5 public
> APIs** (SWEA + SWESTR) and asserted "forecasts 404s on every path" — it had probed the wrong
> base URL. The real Monetary Policy API is `monetary_policy_data/v1/forecasts`, and two more
> products (Turnover, Holdings) existed unwrapped. The full universe is 5 APIs / 156 units.

---

## 1. Documentation compilation

### 1.1 Source inventory

| # | Source | URL | Notes |
|---|--------|-----|-------|
| 1 | Developer portal API list | https://developer.api.riksbank.se/apis | Azure APIM dev portal (SPA). The authoritative list of products. |
| 2 | Portal management API (XHR) | `…/developer/apis?api-version=2022-04-01-preview` | The JSON the SPA consumes — **the authoritative product + operations list** (read via DevTools). Returns 200 unauthenticated. |
| 3 | SWEA docs | https://www.riksbank.se/.../retrieving-interest-rates-and-exchange-rates-via-api/ | + FAQ page (rate limits). |
| 4 | SWESTR docs | https://www.riksbank.se/.../swestr/collecting-swestr-via-api/ | |
| 5 | Monetary Policy docs | https://www.riksbank.se/.../forecasts-and-outcomes/retrieving-forecasts-and-outcomes-via-api/ | SPA — base URL came from the swemo-mcp client + portal. |
| 6 | `swemo-mcp` client | https://github.com/aerugo/swemo-mcp | De-facto spec for the Monetary Policy API: base URL `monetary_policy_data/v1/forecasts`, `/series_ids`, `/policy_rounds`, the colon-`safe=":"` quirk. |

### 1.2 The data model

The five products on the `api.riksbank.se` Azure APIM gateway (paths from portal `/developer/apis`):

| Product (portal name) | Path | Shape | Atomic unit |
|---|---|---|---|
| **SWEA API** | `swea/v1` | JSON series | a series id (`SEKEURPMI`) |
| **SWESTR API** (`tora-api`) | `swestr/v1` | JSON series | a series id (`SWESTR`, `SWESTRAVG*`, `SWESTRINDEX`) |
| **Monetary Policy Data** | `monetary_policy_data/v1` | JSON series × vintages | a forecast series id (`SEQGDPNAYSA`) per `policy_round` |
| **Turnover Statistics** (`selma-api`) | `turnover-statistics/v1` | JSON faceted table | a `(market, frequency)` dataset |
| **Holdings** (`asset-management-api`) | `holdings/v1` | JSON (parquet advertised) | a dataset (`swedish_securities[_aggregated]`) |

- **SWEA series-id suffix** encodes frequency (`PMI`/`PMD` daily, `PMW`/`PMM`/`PMQ`/`PMA`).
- **Monetary Policy series-id** scheme: `COUNTRY-FREQUENCY-AREA-DECOMPOSITION-UNIT-ADJUSTED`
  (e.g. `SEQGDPNAYSA`); 3rd char = frequency (`Q`/`M`/`A`). A **policy round** (`2026:1`) is a
  forecast vintage; each round's observations carry realised history to the forecast cutoff
  plus the forecast horizon. ~59 rounds (`2015:1` → `2026:1`).
- **Turnover** facets: market `fi`/`fx`/`ird` × frequency `daily`/`monthly`; rows are
  `{Period, Asset, Contract, Counterparty, Amount}` (history since 1987).
- **Holdings** rows: `{date, security_group_name(+_se), [issuer_name, security_id, isin,
  maturity_date], balance_nominal_number}`.

### 1.3 Endpoint reference (the load-bearing ones)

| Endpoint | Returns | Notes |
|---|---|---|
| `GET swea/v1/Series` | all ~117 series in one call | the SWEA full index |
| `GET swea/v1/Groups` | group hierarchy | breadcrumb resolution |
| `GET swea/v1/Observations/Latest/{id}` · `…/{id}/{from}/{to}` | obs | latest / window |
| `GET swestr/v1/{latest,all,avg,index}/…` | obs | per-kind URL families |
| `GET monetary_policy_data/v1/forecasts/series_ids` | ~24 series + metadata | the MP full index |
| `GET monetary_policy_data/v1/forecasts/policy_rounds` | ~59 round names | vintage list |
| `GET monetary_policy_data/v1/forecasts?series=<id>&policy_round_name=<round>` | vintages | omit round → all vintages; **colon must be literal** |
| `GET turnover-statistics/v1/markets/{market}/frequencies/{frequency}` | faceted JSON list | history since 1987 |
| `GET holdings/v1/{swedish_securities,…_aggregated}?start_date=` | JSON list | parquet advertised, JSON served |

### 1.4 The "full universe" question

- **Authoritative enumeration path:** the union of (a) SWEA `/Series` (live, 117), (b) the
  SWESTR registry (7, stable), (c) Monetary Policy `/forecasts/series_ids` (live, 24), (d) the
  Turnover market×frequency grid (6, stable), (e) the Holdings dataset set (2, stable).
- **Fan-out:** none for SWEA/MP (single call each); the other three are static.
- **The treasury-trap:** the *whole-API* version of it — three entire products
  (`monetary_policy_data`, `turnover-statistics`, `holdings`) lived on the gateway, unwrapped,
  and one was wrongly documented as "404s everywhere." The authoritative cross-check is the
  portal's own `/developer/apis` list (5 products), not the old code's assumptions.

---

## 2. Authentication & access

- **Keyless.** Optional `Ocp-Apim-Subscription-Key` header raises the quota; env
  `RIKSBANK_API_KEY` (not in the workspace `.env`). No fast-fail — the verbs default
  `api_key=""` and skip the header. Quota: **5 req/min, 1000/day per IP** (so the live test
  suite uses a `RateLimitError`-backoff retry, and the cold catalog build wants a key).

---

## 3. Transport & quirks

- **Base URLs:** five, all on `https://api.riksbank.se/<product>/...`; all JSON → every verb
  reads through `fetch_json` (the one exception below).
- **No anti-bot** (plain httpx; no Akamai/curl_cffi).
- **⚠️ Colon-encoding landmine (Monetary Policy):** a `policy_round_name` is `2026:1`. httpx's
  param encoder turns the colon into `%3A`, which the gateway **404s** on — and the shared
  `HttpClient` also strips a path-embedded query. So `riksbank_monetary_policy_fetch` reads
  through a small raw helper (`_http.get_json_literal_query`) that builds the URL with
  `urlencode(..., safe=":")` and maps errors like `fetch_json`. Without it, a 404 would have
  silently degraded to "return the whole universe unfiltered."
- **Holdings "parquet" is JSON:** the metadata endpoint advertises `file_format: parquet`, but
  the data endpoint serves JSON by default — so no pyarrow dependency.

---

## 4. Catalog plan

- **Strategy:** enumerator + `catalog_build` → `make_local_search_connector`.
- **Namespace:** `riksbank`.
- **Code scheme (routes the fetch):** SWEA/SWESTR keep **bare ids** (self-routing; the SWESTR
  ids start `SWESTR`, and SWEA ids never collide with SWESTR/MP — verified ∅ intersection);
  the three new families carry a routing prefix — `monetary_policy/<id>`,
  `turnover/<market>/<frequency>`, `holdings/<dataset>`. (Prefix chosen because MP ids share
  SWEA's `SED*`/`SEM*`/`SEA*` prefix space, so bare-id routing would be ambiguous.)
- **Entity shape:** KEY=`code` (ns `riksbank`), TITLE=`title`, METADATA=[description, source,
  frequency, unit, group, provider, observation_min, observation_max, series_closed].
- **Index policy:** `discovery_indexes` (code BM25, title/description adaptive), default `title`.

## 5. Connector plan

| Connector | Covers |
|---|---|
| `riksbank_fetch` | a SWEA series (interest/exchange rate) by id |
| `riksbank_swestr_fetch` | a SWESTR fixing / compounded average / index |
| `riksbank_monetary_policy_fetch` | a forecast/outcome series across policy rounds |
| `riksbank_turnover_fetch` | a `(market, frequency)` turnover dataset |
| `riksbank_holdings_fetch` | a securities-holdings dataset |
| `enumerate_riksbank` | the whole 156-unit universe |
| `riksbank_search` | semantic search over the catalog |

- **Deliberately NOT wrapped:** SWEA `CrossRates`/`ObservationAggregates`/`CalendarDays`
  (computed conveniences, no new units); Turnover Excel-report endpoints (duplicate JSON);
  SWESTR `PRESWESTR` (preliminary test data).

## 6. Output schemas

- Fetch outputs each carry KEY=param (`series_id`/`series`/`market`/`dataset`), TITLE, and the
  family DATA columns (date+value, period+amount, date+balance); native extras (SWESTR trade
  metadata, MP `policy_round`/`forecast_cutoff_date`, turnover facets, holdings ISIN/issuer)
  fold in as additional columns.
- Search: `code`/`title`/`score`; the `code` shape routes the follow-up fetch.

## 7. Tests

- `ErrorMappingSuite` on `riksbank_fetch` (keyless). 49 offline (respx) + 9 live integration +
  `test_public_surface` + `test_build_catalog`. `catalog_tests/queries.yaml`: `code` probes for
  all three code shapes + title/hybrid probes.

## 8. Live verification log

| Date | Check | Expected | Actual (live) | Verdict |
|------|-------|----------|---------------|---------|
| 2026-06-10 | product count vs portal | ? | **5** (`/developer/apis`) | ✅ found 3 missing |
| 2026-06-10 | SWEA `/Series` count | ~117 | **117** | ✅ |
| 2026-06-10 | MP `/series_ids` / `/policy_rounds` | — | **24 series / 59 rounds** | ✅ |
| 2026-06-10 | MP single-round filter | 1 series, 1 round | 1 series (189 obs), round `2026:1` | ✅ (after colon fix) |
| 2026-06-10 | MP colon `%3A` | — | **404** (encoded) vs 200 (literal) | ✅ fixed (`safe=":"`) |
| 2026-06-10 | Turnover `fx/monthly` | faceted JSON | 975 rows `{Period,Asset,Contract,Counterparty,Amount}` | ✅ |
| 2026-06-10 | Holdings format | parquet? | **JSON** served (parquet only in metadata) | ✅ no pyarrow |
| 2026-06-10 | full enumerate count | 156 | **156** (0 dup codes) | ✅ |
| 2026-06-10 | search routes families | flagship per family | EUR→SWEA, GDP→`monetary_policy/…`, FX→`turnover/…` | ✅ |

**Completeness sign-off:** the catalog contains all **156** addressable units across the
Riksbank's five public REST APIs (verified by a live keyless enumerate, 0 duplicate codes), and
every product has a fetch verb (exceptions in §5 are computed conveniences / superseded test
data / duplicate Excel). Signed: autonomous, 2026-06-10.

## 9. Open questions / follow-ups

- [ ] Publish the catalog snapshot to `hf://parsimony-dev/riksbank` (156 entries; maintainer
  step, not run — needs a key for a clean cold build given the keyless quota).
- [ ] If finer Turnover granularity is ever wanted, catalog per-`(market, asset)` instead of
  per-`(market, frequency)` (the `definitions` endpoint documents the abbreviations).
