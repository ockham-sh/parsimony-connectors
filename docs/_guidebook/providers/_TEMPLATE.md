# Provider dossier: <PROVIDER NAME> (`<provider_key>`)

> One file per provider/system. This is the **single place** where ALL of the
> provider's documentation, API behaviour, and our findings are compiled before
> a line of connector code is written. Fill it top-to-bottom. Sections 1–3 are
> the slow, careful part — do not rush them. Sections 4–7 are the build.
> Section 8 is the live-verification log that turns "documented" into "proven".
>
> Status legend: 🔲 not started · 🔬 investigating · 🛠 building · ✅ verified-live · ⛔ blocked (needs human)

**Provider:** <full name>  ·  **Key:** `<provider_key>`  ·  **Homepage:** <url>
**Distribution:** `parsimony-<provider_key>`  ·  **Namespace(s):** `<ns>`
**Kind:** commercial-keyed | public-keyless | public-keyed
**Status:** 🔲  ·  **Owner:** <name>  ·  **Last updated:** <YYYY-MM-DD>

---

## 0. TL;DR (fill last)

- What this provider serves, in one sentence:
- Auth: <none | API key via header/query; env var `___`>
- Discovery model: <native search endpoint | we build a catalog | static registry>
- Total addressable universe: <N series / datasets / endpoints> (how we counted: ___)
- Connectors shipped: <list>
- Completeness verdict: catalog covers ALL? <YES/PARTIAL/NO> · connectors cover ALL? <YES/PARTIAL/NO>
- Known gaps / deliberate exclusions: ___

---

## 1. Documentation compilation (THE MOST IMPORTANT STEP)

> Goal: gather EVERYTHING the provider has published about its data and API into
> this one place, with links and verbatim excerpts of the load-bearing parts.
> Documentation is often outdated — capture it anyway, then prove/disprove it in §8.
> Spend the most time here. A connector is only as complete as this section.

### 1.1 Source inventory (link every source you found)

| # | Source | URL | Type | Last seen current? | Notes |
|---|--------|-----|------|--------------------|-------|
| 1 | Official API reference | | api-docs | | |
| 2 | Developer portal / "getting started" | | portal | | |
| 3 | OpenAPI / Swagger spec | | spec | | machine-readable: huge value if it exists |
| 4 | Data catalog / dataset index page | | web | | the human-facing list of what exists |
| 5 | Bulk-download / FTP / S3 manifest | | bulk | | sometimes the only full enumeration |
| 6 | Blog / changelog / release notes | | blog | | reveals undocumented endpoints |
| 7 | Status page / rate-limit docs | | ops | | |
| 8 | Existing client libraries (any language) | | code | | mine for endpoint lists + quirks |
| 9 | Third-party wrappers / community docs | | code | | |
| 10 | Terms of service / acceptable use | | legal | | confirm we may redistribute a catalog |

### 1.2 The data model (in the provider's own terms)

- What is the atomic fetchable unit? (a series? a dataset? a table? an endpoint?)
- How is that unit identified? (id scheme, examples)
- What hierarchy sits above it? (database → series? agency → dataflow → series? dataset → field?)
- What metadata travels with each unit? (title, units, frequency, dates, source, tags)
- Frequencies / dimensions / units universes:

### 1.3 Endpoint reference (verbatim, one row per endpoint)

| Endpoint | Method | Required params | Optional params | Returns | Pagination | Notes |
|----------|--------|-----------------|-----------------|---------|------------|-------|
| | | | | | | |

### 1.4 The "full universe" question — how is EVERYTHING listed?

> This is the crux of catalog completeness. Identify the *authoritative*
> enumeration path: the endpoint(s)/file(s) that, walked exhaustively, yield
> EVERY addressable unit. If no single endpoint lists everything, write down the
> union of sources that does — and what (if anything) is unreachable.

- Authoritative enumeration path: ___
- Does it require pagination / recursion / per-parent fan-out? ___
- Estimated total count (and how the provider states it, if it does): ___
- Things that exist but are NOT in the enumeration endpoint (the treasury-trap —
  famous series living on a different subdomain, bulk-only datasets, etc.): ___
- Anything gated behind a higher plan / login we cannot reach: ___

---

## 2. Authentication & access

- Auth required? <yes/no>. Mechanism: <none | api_key query param `___` | header `___` | bearer | OAuth>
- How to obtain a key (URL + steps):
- Free tier limits / rate limits / quotas:
- ⛔ **Human intervention needed?** If a key/login is required, STOP and ask the
  user to create it and add it to `ockham/.env` as `<ENV_VAR>`. Record the ask here:
  - Asked on: ___ · Provided: ___ · Env var: `<PROVIDER>_API_KEY`
- Secret handling plan: `secrets=("api_key",)`, env fallback `<PROVIDER>_API_KEY`,
  fail-fast `UnauthorizedError("<provider>", env_var="<PROVIDER>_API_KEY")`.

---

## 3. Transport & quirks

- Base URL(s):
- Response format(s): JSON | XML | CSV | SDMX-ML | other
- Can `fetch_json` handle it, or do we need raw `HttpClient` + custom parse? (XML/CSV ⇒ raw)
- Pagination strategy:
- Rate-limit headers / backoff behaviour:
- Anti-bot / TLS-fingerprinting (Akamai/Cloudflare ⇒ may need `curl_cffi`)?
- Date/number formats, locale traps, encoding:
- Other landmines:

---

## 4. Catalog plan (skip if the provider has a native search endpoint)

- Strategy: none(native search) | flat | enumerator+catalog_build | multi-bundle | structured-dimensions
- Namespace(s): `<ns>`
- Code scheme (how a catalog code is formed, e.g. `{endpoint}#{field}`):
- Entity shape: KEY=`<col>` (namespace `<ns>`), TITLE=`<col>`, METADATA=[...]
- Enumeration code: how `enumerate_<provider>` / `build_<provider>_catalog` walks §1.4's
  authoritative path to emit ONE row per addressable unit:
- Index policy: `discovery_indexes()` | custom | bm25-only (cardinality reason):
- Multi-bundle? (per-agency / per-database split to bound embedding memory): <no/how>
- Catalog URL: `hf://parsimony-dev/<provider>` · env override `PARSIMONY_<PROVIDER>_CATALOG_URL`

## 5. Connector plan

| Connector | Decorator | Tool? | Purpose | Covers which slice of the universe |
|-----------|-----------|-------|---------|-------------------------------------|
| `<p>_search` | @connector (make_local_search_connector) | yes | discovery | the whole catalog |
| `<p>_fetch` | @connector / @loader | no | bulk fetch | |
| `enumerate_<p>` | @enumerator | no | catalog feed | the whole universe |

- Endpoints deliberately NOT wrapped (and why): ___

## 6. Output schemas

- Fetch `OutputConfig`: KEY=..., DATA=..., dtypes=...
- Search `OutputConfig`: code/title/score (+ any dispatch metadata the agent needs to route a fetch)
- Compound-code / dispatch notes (how search results map back to a fetch call):

---

## 7. Tests

- `ErrorMappingSuite` wired (route_url, method, env_key, provider): ___
- `IntegrationSuite` (live, env-gated) smoke: ___
- Conformance (`ProviderTestSuite` / `assert_plugin_valid`): ___
- Catalog probes `catalog_tests/queries.yaml` (modes: code / title_bm25 / hybrid_title / structured_field):

---

## 8. Live verification log (documentation is a claim; execution is the truth)

> The source of truth is running the calls yourself. Every row here is a claim
> from §1 that you confirmed or refuted against the live endpoint. Catalogs are
> only "complete" once the live count matches §1.4's expected total.

| Date | Check | Expected (from docs) | Actual (live) | Verdict | Action |
|------|-------|----------------------|---------------|---------|--------|
| | auth: missing key → 401 mapped to UnauthorizedError | | | | |
| | enumeration count vs documented total | | | | |
| | sample fetch returns parseable rows | | | | |
| | a known famous series is findable in the catalog | | | | |
| | rate-limit header parsed into RateLimitError.retry_after | | | | |

**Completeness sign-off:** the catalog contains ALL <N> addressable units
(verified by: ___) and the connectors expose every accessible data class
(exceptions documented in §5). Signed: ___ on ___.

---

## 9. Open questions / follow-ups

- [ ]
