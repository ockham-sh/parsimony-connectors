# The Parsimony Connector Guidebook

> A working manual for connecting to data providers with the parsimony framework, and a
> living record of findings as new providers are tackled. Built from a first-hand read of
> `parsimony-core` (source + `parsimony/docs/`), all 24 existing connector packages, and the
> 0.7 sweep dossiers. Companion to the per-provider research dossiers in
> [`providers/`](providers/) (template: [`providers/_TEMPLATE.md`](providers/_TEMPLATE.md)).
>
> **The two questions this guidebook exists to answer, for every provider:**
> 1. Does the **catalog** contain **ALL** available series the provider exposes?
> 2. Do the **connectors** cover **ALL** accessible data?
>
> Neither is answerable from documentation alone. Documentation is a *claim*; the source of
> truth is **executing the calls yourself**. Compile every scrap of provider documentation
> first, build from it, then prove it against the live API.

---

## Table of contents

1. [How to use this guidebook (the process)](#1-how-to-use-this-guidebook-the-process)
2. [Mental model](#2-mental-model)
3. [The decision tree: what kind of connector am I building?](#3-the-decision-tree)
4. [Framework contract (reference)](#4-framework-contract-reference)
5. [Authentication recipes](#5-authentication-recipes)
6. [Transport recipes](#6-transport-recipes)
7. [Discovery & catalogs — the completeness heart](#7-discovery--catalogs--the-completeness-heart)
8. [The completeness mandate (live verification)](#8-the-completeness-mandate-live-verification)
9. [Testing](#9-testing)
10. [Packaging, registration, release](#10-packaging-registration-release)
11. [Connector roster & archetype index (which exemplar do I copy?)](#11-connector-roster--archetype-index)
12. [Cross-cutting gotchas](#12-cross-cutting-gotchas)
13. [Running findings log](#13-running-findings-log)

---

## 1. How to use this guidebook (the process)

Building a connector that genuinely covers all of a provider's data is a research task before
it is a coding task. The order matters:

1. **Open a dossier.** Copy [`providers/_TEMPLATE.md`](providers/_TEMPLATE.md) to
   `providers/<provider_key>.md`. This is your single place for everything below.

2. **Compile ALL the documentation first (the slowest, most important step).** Fill the
   dossier's §1: official API reference, developer portal, OpenAPI/Swagger spec if one exists,
   the human-facing data-catalog/dataset-index pages, bulk-download manifests, blog/changelog
   posts that reveal undocumented endpoints, existing client libraries in any language, the
   status/rate-limit page, and the ToS. Quote the load-bearing parts verbatim. **The single
   most valuable artifact you can produce here is the answer to §1.4: what is the one
   authoritative enumeration path that, walked exhaustively, lists EVERY addressable unit?**
   A catalog is only as complete as that answer.

3. **Settle auth (§2 of the dossier).** Decide the mechanism, the env-var name, whether a key
   is required vs optional. **If a key or login is required, STOP and ask the user to create
   it and add it to their env.** Record the ask in the dossier and resume once it lands.
   (`bdf` *was* the cautionary case — shipped `UNVERIFIED-LIVE` on hand-authored mocks — until
   2026-06-08, when the key was found already present under a non-convention name; see §13. The
   principle stands: don't ship `UNVERIFIED-LIVE`, and grep `.env` for the provider under any
   name before assuming no key exists.)

4. **Build from the documentation:** the enumerator/catalog and the fetch connectors. Use the
   archetype index (§11) to find the closest existing connector and copy its structure.

5. **Prove it live (§8 of the dossier).** Documentation is outdated more often than not.
   Run every connector against the real API. Assert *real values*, not just column names.
   Reconcile the catalog's live entry count against §1.4's expected total. Log every claim you
   confirmed or refuted in the dossier's live-verification table. A catalog is "complete" only
   once the live count matches the documented universe.

6. **Fold findings back here.** When you learn something transferable (a new completeness
   technique, a provider quirk worth a pattern, a gotcha), append it to §13 and, if it changes
   a rule, edit the relevant section. This guidebook compounds.

---

## 2. Mental model

**A connector is a small async Python function plus metadata.** You write an `async def` that
fetches and shapes raw data and `return`s it (a `pandas.DataFrame`/`Series`, a scalar, or a
`dict`). The framework wraps that return value into a `Result`/`TabularResult`, attaches
framework-built `Provenance`, and strips declared secrets. You never construct `Result`,
`TabularResult`, `Provenance`, or `Connector` yourself, and you never return a `(data, props)`
tuple (both raise `TypeError`).

**Two jobs, always separate the verbs by them:**

- **Discovery** — *what data exists?* Either the provider has a native search endpoint (you
  wrap it), or it doesn't and you must **build a catalog**: a searchable index of every
  addressable unit, with titles and metadata, that an agent queries to find a code, then
  passes that code to a fetch connector.
- **Fetch** — *give me the values for this code.* A bulk-fetch connector that returns
  observations into a code interpreter variable.

**The catalog is the completeness surface for discovery.** When a provider has no search API,
the only way an agent can find all the data is if you have enumerated all of it into a
catalog. This is where most of the difficulty (and creativity) lives, and where the "does it
cover ALL the data?" question is won or lost.

**`parsimony-core` ships zero connectors.** Each provider is its own pip-installable
distribution (`parsimony-<name>`) discovered at runtime through a Python entry point. The
kernel is a thin, provider-agnostic shell: connector primitives, the catalog/search engine,
typed errors, an HTTP transport layer, and a conformance suite. **Every provider quirk lives
in the connector, never in the kernel.**

---

## 3. The decision tree

```
Does the provider expose a usable native SEARCH endpoint
(keyword/symbol search, screener, or a single full-universe bulk list)?
│
├─ YES → wrap that search endpoint as a 'tool'-tagged @connector. DO NOT build a catalog.
│        (Optionally ship one @enumerator over the provider's bulk symbol list for a
│         downstream/external indexer — bound its output if huge.)
│        Exemplars: alpha_vantage, fmp, coingecko, finnhub, eodhd, tiingo, fred,
│                   polymarket, sec_edgar  →  these 9 are EXCLUDED_COMMERCIAL_PROVIDERS
│                   in tooling/catalog_validate/registry.py.
│
└─ NO  → BUILD A CATALOG (enumerator → catalog_build → build_catalog.py → search connector).
         This is the only path where the "catalog covers ALL series" question is live.
         Exemplars: treasury, bde, bdf, bdp, boc, snb, rba, riksbank, destatis, boj, sdmx.

Orthogonal axis — authentication:
  • Required key      → §4.3 keyed triad, fast-fail UnauthorizedError before any network call.
                        (alpha_vantage, fmp, coingecko, finnhub, eodhd, tiingo, fred, eia, bdf)
  • Optional key      → key only raises quota (and may enrich output); never fast-fail.
                        (bls registrationkey; riksbank Ocp-Apim key)
  • Keyless           → no secrets=, no bind/load(api_key), no UnauthorizedError on the data path.
                        (treasury, bde, bdp, boc, snb, rba, destatis, boj, sdmx, polymarket)
  • Keyless + required header → a non-secret infra header with a pre-network fast-fail.
                        (sec_edgar User-Agent)
```

**Do not catalog a provider that already has search** (it is wasted work and a stale duplicate
of the provider's own index), and **do not catalog endpoints that 404** (they mislead the
dispatching agent). Build a catalog only when the provider lacks native discovery *and* the
series set is enumerable.

---

## 4. Framework contract (reference)

Everything here is `parsimony-core`. Imports: top-level `from parsimony import …` exposes the
~35 curated names; some names live only in submodules (noted below).

### 4.1 The three decorators

| Decorator | Output contract | Feeds | Use for |
|---|---|---|---|
| `@connector` | `output=` optional; unmapped returned columns fold in as DATA | anything | fetches, native-search wrappers, dict/scalar lookups |
| `@loader(output=…)` | **exactly one** namespaced KEY + **≥1 DATA**, **no** TITLE/METADATA | a data store (`InMemoryDataStore.load_result`) | persisting observation values |
| `@enumerator(output=…)` | **exactly one** namespaced KEY + **≥1 TITLE**, **no DATA**; must annotate `-> pd.DataFrame` | a `Catalog` | discovering *what entities exist* |

- All three decorate an `async def`. A plain `def` raises `TypeError` at decoration.
- Parameters are **flat top-level scalars**. Never expose a public parameter literally named
  `params` annotated as a Pydantic `BaseModel` (conformance check `check_flat_public_params`).
  Internal Pydantic validation is fine.
- **Return raw data only.** The framework builds the envelope. Provider facts go in DataFrame
  columns, never in `provenance.properties` / `with_properties` (those are framework/
  serialization affordances).
- A **description is required** (docstring or `description=`), stripped length **20–800**
  chars. It is the LLM-facing capability statement. Tool-tagged connectors want a ≥40-char
  first sentence (it becomes the agent/MCP tool description).
- `@loader` prepends the `"loader"` tag; `@enumerator` prepends `"enumerator"` and stamps a
  role marker on the function (a faked `@connector(tags=["enumerator"])` fails conformance).
- **In practice almost every "fetch" in the repo is a plain `@connector`, not `@loader`** —
  because real fetches carry a human-readable TITLE column alongside DATA, which `@loader`
  forbids. Reach for `@loader` only when you are literally feeding `InMemoryDataStore`. The
  **kind is chosen by intent, not just column shape** (`sec_edgar_find_company` is
  KEY+TITLE-shaped but stays `@connector` because it carries a DATA column; document the
  reasoning inline).

**Validation timing** (most failures surface at import, not at call):

| Check | When |
|---|---|
| loader/enumerator role-shape, enumerator return annotation, `secrets=` names match params, async-ness | decoration (import) |
| `OutputConfig` "≤1 KEY / ≤1 TITLE / ≥1 KEY-or-TITLE-or-DATA" | `OutputConfig(...)` construction |
| enumerator returned-frame **exact** column match | every call (`ValueError` → `ParseError`) |
| returned a `Result`/`TabularResult`/tuple | every call (`TypeError`) |

### 4.2 Output schema: `OutputConfig`, `Column`, `ColumnRole`

```python
from parsimony.result import Column, ColumnRole, OutputConfig
OutputConfig(columns=[
    Column(name="series_id", role=ColumnRole.KEY, namespace="fred"),   # identity
    Column(name="title",     role=ColumnRole.TITLE),                   # human label
    Column(name="units",     role=ColumnRole.METADATA),               # searchable facet
    Column(name="date",      role=ColumnRole.DATA, dtype="datetime"),
    Column(name="value",     role=ColumnRole.DATA, dtype="numeric"),
])
```

- `ColumnRole` ∈ {`KEY`, `TITLE`, `METADATA`, `DATA`}. At most one KEY, at most one TITLE,
  at least one of DATA/KEY/TITLE.
- `namespace=` is allowed **only** on KEY or METADATA columns and must be non-empty. The KEY
  namespace is **mandatory** for loaders/enumerators — the catalog/store derive entity
  identity from it.
- `dtype` ∈ `auto | datetime | timestamp | date | numeric | bool | <any pandas dtype>`.
  `timestamp` expects **unix epoch seconds** (it divides ms-looking values by 1000).
- `Column(name="*")` is a **wildcard** that absorbs every returned column not otherwise
  claimed — use it (as METADATA on an enumerator, or as a catch-all DATA on a wide fetch like
  a 40-field financial statement) when columns vary per call.
- `exclude_from_llm_view=True` (KEY/METADATA only) keeps a noisy column out of the agent view
  while retaining it in data.
- **`@connector`/`@loader` fold unmapped returned columns in as DATA** (`merge_unmapped_as_data=
  True`). So `return df[[c.name for c in OUTPUT.columns]]` if you don't want provider
  passthrough columns (e.g. FRED's stray `realtime_start`). **`@enumerator` does the opposite**:
  it drops unmapped columns, then requires an **exact** match — declare *every* column you want
  in the catalog and return exactly those.

### 4.3 `Result` / `Provenance`

The framework builds `Provenance(source=<connector name>, source_description=<description>,
fetched_at=<utc now>, params=<call args minus declared secrets and minus bound args>)`. A
`TabularResult` round-trips to Arrow/Parquet with the schema + provenance embedded. Oversized
provenance fields are replaced with a structured marker (never a prefix, which could leak a
secret head).

### 4.4 Credentials: `secrets=` + `bind`

Two **independent** mechanisms — combine both:

- `secrets=("api_key",)` on the decorator → strips the named param from `provenance.params`.
  (Names are validated against real parameters at decoration; an unknown name raises.)
- `Connector.bind(api_key=…)` → fixes the value and removes it from the exposed signature, the
  `describe()`/`to_llm()` cards, and binding. This is how an operator wires a key without
  exposing it to an agent.

The standard per-package entry point: `def load(*, api_key): return CONNECTORS.bind(api_key=api_key)`.

`Connectors` is an immutable, name-keyed collection (`bundle["name"]`, not `bundle[0]`; merge
with `+`; `.bind(...)` scopes per-connector). Link a fetch param to a catalog namespace for
the LLM card with `Annotated[str, "ns:<namespace>"]`.

### 4.5 Typed errors (`parsimony.errors`)

Operational failures only — programmer errors stay native (`TypeError`/`ValueError`/Pydantic
`ValidationError`). Each carries `provider`; the **default message is the canonical
agent-facing string** with the right "DO NOT retry" directive.

| Error | Raise for | Notes |
|---|---|---|
| `UnauthorizedError(provider, env_var=…)` | 401/403 bad/missing creds | `env_var` is **keyword-only**; names the var the agent should set |
| `PaymentRequiredError(provider)` | 402 / plan-tier restriction | even when the upstream status is a 401/403 whose *body* says "plan" |
| `RateLimitError(provider, retry_after, *, quota_exhausted=False)` | 429 | `retry_after` is **seconds** (>86400 raises); `quota_exhausted=True` = billing-period |
| `ProviderError(provider, status_code)` | 5xx / 404 / timeout(408) | carries the status |
| `EmptyDataError(provider, query_params=…)` | 200 with zero rows | recovery path is adjust-params; no "DO NOT retry" |
| `ParseError(provider, msg=None)` | 200 but unparseable / schema drift | the honest mapping for a 200-with-error-body |
| `InvalidParameterError(provider, message)` | bad call-time arg (pre-network) | `message` required |
| `CatalogNotFoundError(msg)` | catalog bundle missing | import from `parsimony.errors`, not top-level |

**The argument-order trap:** the base `ConnectorError(message, *, provider)` takes message
first, but **every typed subclass flips it** — `provider` is the first positional.
`UnauthorizedError("some text")` sets `provider="some text"`. Always
`UnauthorizedError("fred", env_var="FRED_API_KEY")`. **Branch on attributes/codes, never on
message strings** (strings are for the agent). Never put URLs/tokens/raw upstream prose in a
`message=` override (leak + prompt-injection risk; the override gate is truthiness, so an
empty string falls through to the default).

### 4.6 Transport (`parsimony.transport` + `parsimony.transport.helpers`)

- `make_http_client(base_url, *, query_params=None, headers=None, timeout=15.0)` /
  `make_api_key_client(base_url, *, api_key, api_key_param="apikey", timeout=15.0)` build a
  configured `HttpClient`.
- `fetch_json(http, *, path, params, provider, op_name)` does it all for GET+JSON: drops
  `None` params, GET, `raise_for_status()`, maps `httpx` errors to typed errors
  (`map_http_error` / `map_timeout_error`), returns parsed JSON.
- `HttpClient.request()` **never calls `raise_for_status()`** — a 503 looks like a 200. If you
  use it raw, call `raise_for_status()` yourself and feed the error to `map_http_error` /
  `map_timeout_error`.
- `HttpClient` auto-redacts query-param **values** whose **name** is in the sensitive set
  (`api_key`, `apikey`, `api_token`, `token`, `access_token`, `refresh_token`, `id_token`,
  `client_secret`, `secret`, `password`, `authorization`, `registrationkey`, plus any
  `*_token`). A key sent as a query param under a name **not** in this set leaks to INFO logs
  (`secrets=()` governs provenance, **not** logs). Fixes: send it in a header or POST body, or
  add the name to `_SENSITIVE_QUERY_PARAM_NAMES` (a parsimony-core change — what BLS's
  `registrationkey` needed).
- `pooled_client(http)` yields a client backed by one pooled `AsyncClient` for fan-out /
  enumerator loops. Built-in transient retry policy (GET/HEAD/OPTIONS; 429/5xx; exp backoff).

### 4.7 Discovery & conformance

- Register `[project.entry-points."parsimony.providers"]` with `<provider> = "parsimony_<name>"`.
  **Verified against `discover.py`: the kernel does `importlib.import_module(ep.value)` then
  `getattr(mod, "CONNECTORS")`, so the value must be a bare importable module path — NOT
  `parsimony_<name>:CONNECTORS`.** (`CONTRIBUTING.md` currently shows the `:CONNECTORS` form;
  it is stale. Trust the code.)
- The module must export a top-level `CONNECTORS = Connectors([...])` — non-empty, never a
  bare list, never renamed.
- Conformance = the merge/release gate. `parsimony.testing.assert_plugin_valid(module)` (and
  `parsimony list --strict`) run five fail-fast checks: connectors-exported, descriptions
  20–800, enumerator-decorator (real role marker), enumerator-return-type, flat-public-params.
- Only two `PARSIMONY_*` env vars exist in core: `PARSIMONY_CACHE_DIR` (relocates the whole
  cache root) and `PARSIMONY_FAISS_IVF_THRESHOLD`. **Core never reads provider creds from
  fixed `PARSIMONY_*` names** — your connector reads its own `<PROVIDER>_API_KEY`. A connector
  *may* read its catalog-URL override from a per-connector var of its own choosing
  (convention: `PARSIMONY_<PROVIDER>_CATALOG_URL`).

---

## 5. Authentication recipes

### 5.1 The keyed triad (required key) — the north star

Copy this verbatim per keyed connector:

```python
_ENV_VAR = "ACME_API_KEY"

def _client(api_key: str) -> HttpClient:
    key = api_key or os.environ.get(_ENV_VAR, "")
    if not key:
        raise UnauthorizedError("acme", env_var=_ENV_VAR)   # fast-fail BEFORE any network call
    return make_http_client(_BASE_URL, query_params={"apikey": key, "fmt": "json"})

@connector(output=…, tags=["…", "tool"], secrets=("api_key",))
async def acme_search(query: str, api_key: str = "") -> pd.DataFrame: ...

def load(*, api_key: str) -> Connectors:
    return CONNECTORS.bind(api_key=api_key)
```

Rules: `secrets=("api_key",)` on **every** verb (including the enumerator); a single shared
`_client()` so one parametrized no-key test covers all verbs symmetrically; `env_var` is
keyword-only and conventionally `<PROVIDER>_API_KEY`.

**Pick the helper by the key's param name:** `make_api_key_client` hardcodes
`api_key_param="apikey"` and sets *only* the key, so if the param is anything else (FRED
`api_key`, EIA `api_key`, EODHD `api_token`) **or** you need an extra fixed query param
(`file_type=json`, `fmt=json`), use `make_http_client(query_params={...})` instead.

### 5.2 Carry the key the way the provider expects — prefer header/body over query

| Style | Carrier | Exemplar | Note |
|---|---|---|---|
| query param | `?apikey=` / `?api_token=` / `?token=` | alpha_vantage, fmp, eodhd, finnhub, fred, eia | only redacted if the param name is in the sensitive set |
| custom header | `Authorization: Token <key>` | tiingo | never lands in a URL/log |
| custom header | `x-cg-demo-api-key` | coingecko | never logged (headers aren't logged) |
| Opendatasoft header | `Authorization: Apikey <key>` (literal word "Apikey", **not** Bearer) | bdf | wrong scheme → silent 401 |
| Azure APIM header | `Ocp-Apim-Subscription-Key` | riksbank | optional quota-raiser |

Prefer a header or POST body so the key never enters a query string or log.

### 5.3 Optional key (key only raises quota / enriches output)

`bls`: `registrationkey` is optional, defaults to `""`, is **never** fast-failed; the body
sets it only `if api_key`. Note the subtlety — with a key, BLS returns real series *titles*;
without, the title falls back to the series id. So the key changes **output shape**, not just
rate limits. Still declare `secrets=("api_key",)`. Guard keyless leak-checks with `if _KEY:`.

### 5.4 Keyless, and keyless-but-header-required

- Pure keyless (`treasury`, `bde`, `bdp`, `boc`, `snb`, `rba`, `destatis`, `boj`, `sdmx`,
  `polymarket`): no `secrets=`, no `bind`/`load(api_key=)`, no `UnauthorizedError` on the data
  path. `load(*, catalog_url=None)` binds only the catalog URL. Integration tests skip
  `assert_no_secret_leak` (no key to leak).
- **"Keyless" is not binary.** `sec_edgar` is keyless but SEC's fair-access policy *requires* a
  `User-Agent` header (name+email) or it 403/429s. Model it as a **non-secret** env-resolved
  header (`SEC_EDGAR_USER_AGENT`) with a pre-network fast-fail (`UnauthorizedError(env_var=…)`),
  and deliberately **not** via `secrets=`/`bind`/`load` (a header isn't logged/redacted).
  Many providers also need a browser `User-Agent` just to avoid a 301 to their SPA shell
  (`destatis`) or to get past a WAF (`bdp`, `boj`).

### 5.5 Dual-meaning status disambiguation (the #1 auth gotcha)

When **one** status means *both* bad-key and plan-restriction, you must look at the **body**:

| Provider | Overloaded status | Disambiguator |
|---|---|---|
| coingecko | **401** = bad key OR plan | numeric `error_code` (`{10005,10006,10012}` → plan); body has two nesting shapes |
| tiingo | **403** = invalid/absent token OR plan | body contains `permission`/`news api` → plan, else fall through → Unauthorized |

When the statuses are **distinct**, map by status alone (simpler, correct):

| Provider | Bad key | Plan restriction |
|---|---|---|
| finnhub | 401 | 403 → Payment |
| fmp | 401 | 402 / 403 → Payment |
| eodhd | 401 | 403 / **423 Locked** → Payment |

**Present-but-invalid key is frequently NOT 401** — an *absent* key fast-fails to
`UnauthorizedError` in `_client()`; a *present-but-invalid* key reaches the wire and the
provider decides (FRED returns **400** → `ProviderError(400)`). Don't assume 401 in tests.

### 5.6 Human-intervention protocol (asking for a key)

If a key or login is required and isn't in `ockham/.env`, **stop and ask the user** to create
it and add `<PROVIDER>_API_KEY=...`. Record the ask in the dossier. Until it lands, the
connector is `UNVERIFIED-LIVE` — say so explicitly and treat host/path/shape as unconfirmed
(the former `bdf` situation, resolved 2026-06-08 — see §13). Never echo a key value (`${VAR:-X}` prints it); test presence with
`[ -n "$VAR" ]` / `${VAR:+SET}`.

---

## 6. Transport recipes

**Decision tree:**

```
GET + JSON, canonical status semantics?           → fetch_json (default; do this)
POST + JSON?                                       → _post_json helper (NEVER retried; non-idempotent)
GET + XML / CSV / text?                            → _get_text helper, parse separately
Status semantics differ from the canonical table?  → raw HttpClient + a single per-package mapper chokepoint
Host TLS-fingerprint-blocks httpx (Akamai/CF)?     → curl_cffi (impersonate='chrome') + hand-written mapper
Bulk metadata fan-out (catalog crawl)?             → parsimony_shared.cb_enumerate.ThrottledJsonFetcher
Many requests in one logical op (screener/loop)?   → pooled_client(http)
```

### 6.1 The canonical siblings (`_post_json`, `_get_text`)

`fetch_json` is GET+JSON only. The two siblings (define locally; `_get_text` is written for
reuse across XML/text feeds):

```python
async def _get_text(http, path, *, params, op_name):                # XML/CSV/text GET
    try:
        r = await http.request("GET", path, params=params); r.raise_for_status()
    except httpx.HTTPStatusError as e: map_http_error(e, provider="…", op_name=op_name)
    except httpx.TimeoutException as e: map_timeout_error(e, provider="…", op_name=op_name)
    return r.text

async def _post_json(http, path, *, payload, op_name):              # POST JSON (no retry)
    try:
        r = await http.request("POST", path, json=payload); r.raise_for_status()
    except httpx.HTTPStatusError as e: map_http_error(e, provider="…", op_name=op_name)
    except httpx.TimeoutException as e: map_timeout_error(e, provider="…", op_name=op_name)
    return r.json()
```

A malformed body on a 200 → `ParseError` (see §6.4). Put credentials in the POST **body**, not
a query param.

### 6.2 Per-package mapper chokepoint (non-canonical status)

When a provider's statuses diverge, drop below `fetch_json` to **one** hand-written chokepoint
that every verb (including the enumerator) routes through — never special-case per verb. It
drops `None` params, `raise_for_status()`, applies the provider-specific branch, then falls
through to `map_http_error`/`map_timeout_error`. Reference implementations: `fmp_get` (402/403
→ Payment by status alone), `eodhd_get` (403/423 → Payment), `coingecko_fetch`/`finnhub_get`
(401/403 dual-meaning by body). Keeping it a single chokepoint means error-mapping + secret
redaction are guaranteed identical across the whole surface.

### 6.3 Akamai / Cloudflare TLS-fingerprint blocking → `curl_cffi`

Some hosts 403 stock `httpx` on TLS fingerprint, so the canonical transport **structurally
cannot** reach them. `rba` is the reference: `curl_cffi.AsyncSession.get(url,
impersonate="chrome")` as a **hard** dependency + a hand-written `_curl_get` mapper that
mirrors `map_http_error`/`map_timeout_error` (429→RateLimit, 402→Payment, 401/403→Unauthorized,
other→Provider(status); `curl_cffi` Timeout/RequestException→Provider(408)). Pool one
`AsyncSession` across the fan-out under a `Semaphore`. **Never weaken the typed-error contract
just because the transport changed.** Keep the impersonation target current (old Chrome
fingerprints start to 403). Lighter WAFs (`bdp`, `boj`) need only a browser `User-Agent` +
`Origin`/`Referer` and `403` added to the retry-status set.

### 6.4 200-with-error-body (the statistical-office quirk)

Many providers (BLS, SDMX, destatis, alpha_vantage) return **HTTP 200** carrying a logical
failure — an HTML SPA/maintenance shell, a throttle notice, or an error envelope. Read the
body, then map honestly: a quota/threshold phrase → `RateLimitError(quota_exhausted=True)`;
anything else unrecognized → `ParseError` (it carries no status to falsify). **Never invent
`ProviderError(status_code=0)`.** String-sniffing the message is tolerated **only** when the
provider exposes no machine-readable error code (alpha_vantage's `Information` body is
byte-identical for quota vs premium, so it maps to the safer `RateLimitError`); document why.

### 6.5 Bulk metadata crawls (`parsimony_shared.cb_enumerate`)

For catalog enumeration fan-outs, use the shared `ThrottledJsonFetcher` +
`MetadataCrawlConfig` (semaphore concurrency, inter-request delay, retry-with-backoff on
429/5xx + `Retry-After`, **best-effort returns `None` on failure**). `get_json`/`get_text`/
`get_content` variants. `destatis`/`bde`/`bdf`/`bdp`/`boj` reuse it; tune concurrency to the
provider's tolerance (destatis = 4 @ 0.25s; higher triggers 429/503). `boc`/`snb` hand-roll an
equivalent `asyncio.gather` + `Semaphore` inside `pooled_client`. Also in the kit:
`truncate_description`/`enumerate_descriptions` (cap at `DESCRIPTION_CHAR_CAP=1500` for
embedder friendliness).

### 6.6 Parsing real-world payloads

- **Coerce only the measure column** to numeric. `pd.to_numeric(errors="coerce")` over every
  column silently NaNs string metadata (EIA `duoarea`/`product`, SNB dimension codes). A
  `dtype="numeric"` column that becomes **all-NaN** raises `ParseError` (the framework's
  all-NaN guard) — FRED encodes missing as the sentinel `"."`, so pick real-data windows in
  live tests.
- **Multi-host providers:** per-base-URL clients with the boundary documented in code
  (`sec_edgar`: `data.sec.gov` for JSON APIs, `www.sec.gov` for the ticker map + `/Archives`
  bodies — `data.sec.gov` 404s `/Archives`).
- **Date/period normalization** is a frequent silent-data-loss trap: BLS `M13` (annual avg) →
  `2024-13-01` → NaT; OData feeds use `NEW_DATE`/`INDEX_DATE`/`QUOTE_DATE` (normalize via an
  ordered tuple, first-present-wins, into a uniform `record_date`); BoJ widths are
  frequency-derived (`YYYYMMDD`/`YYYYMM`/`YYYYQQ`/`YYYY`); destatis German labels
  (`Januar 2026`, `1. Quartal 2026`). Pass unknown widths through unchanged so a real parse
  error surfaces instead of silent mangling. Split host-from-path to dodge trailing-slash 301s.
- **Encodings:** `bde` CSVs are CP1252 (latin-1 fallback); UTF-8 corrupts `Tipo de interés`.
- **JSON-stat 2.0** (destatis, bdp): expand the flat `value` array (list or sparse
  `{index:value}` dict) across the N-dim cartesian product from `id`/`size`; pick the time
  dimension by name heuristic; drop null cells; all-null → `EmptyDataError`.

---

## 7. Discovery & catalogs — the completeness heart

This is where "does the catalog contain ALL the data?" is won. Read it carefully.

### 7.1 The catalog pipeline (the 5-call builder recipe)

When a provider has no native search, three roles, cleanly separated:

1. **`@enumerator`** in `parsimony_<p>/__init__.py` (or a submodule) — emits **one row per
   addressable unit** (KEY=code+namespace, TITLE, METADATA). This is the completeness surface.
2. **`catalog_build.py:build_<p>_catalog()`** — the reusable async builder (the literal recipe):

   ```python
   async def build_<p>_catalog(...) -> Catalog:
       result   = await enumerate_<p>(...)                          # the @enumerator
       entries  = entities_from_raw(result, <P>_ENUMERATE_OUTPUT)   # DataFrame → list[Entity]
       catalog  = Catalog(NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
       catalog.set_entities(entries)
       await catalog.build()
       return catalog
   ```
3. **`scripts/build_catalog.py`** — the **operator** CLI (`argparse --save file://… / --push
   hf://…`, `--api-key` fallback to env), which calls `catalog.save(url,
   builder="packages/<p>/scripts/build_catalog.py")`. **Build scripts are maintainer tooling,
   NOT part of the plugin contract** — the user-facing surface is `CONNECTORS`. Never enumerate
   or download at import time.

The search connector is one declarative call (`search.py`):

```python
<p>_search = make_local_search_connector(
    provider="<p>", default_url="hf://parsimony-dev/<p>",
    catalog_url_env_var="PARSIMONY_<P>_CATALOG_URL",
    build_catalog=build_<p>_catalog,            # cold-build fallback
    tags=["…", "tool"], description="…dispatch routing each hit to its fetch connector…",
    output_columns=[code(KEY), title(TITLE), score(DATA)])
```

Resolution order is `params.catalog_url` → env var → `default_url`; on a missing remote it
falls back to the on-disk lazy cache, then rebuilds via `build_catalog`. So search works in
prod (published HF snapshot) and in a fresh dev clone (local rebuild) with no code change.
`CatalogLRU` caches hydrated catalogs in-process. **Registration stays network-free** because
`Catalog.load` is lazy.

`CONNECTORS = Connectors([<fetch…>, enumerate_<p>, <p>_search])` — fetch + enumerator + search.

### 7.2 Catalog enumeration archetypes (how to list the FULL universe)

The whole completeness game is identifying the **authoritative enumeration path**. Six
archetypes, in rough order of preference:

| # | Archetype | How | Exemplars | Self-tracks new data? |
|---|---|---|---|---|
| A | **Live full-index endpoint** | one call returns the entire universe; rebuild every refresh | boc `/lists/series` (~15.6k), destatis `/statistics` (~331), bdf `series` flat export (~41.6k), riksbank SWEA `/Series` (~117) **+** Monetary Policy `/forecasts/series_ids` (~24), sdmx `client.dataflow(force=True)` per agency, **snb `/sitemap` (1,149 cubes — XML, in `robots.txt`)**, **treasury `/services/dtg/metadata/` (56 datasets → 879 measures, the JSON the fiscaldata SPA consumes)** | **Yes** — preferred |
| B | **Crawl the hierarchy (fan-out)** | walk parent→child with bounded concurrency; **paginate every list level**; 1+N or 1+2N | bdp domains→**paginated datasets**→**paginated** series (~72k; ~720 pages at `page_size=100&obs_last_n=1`), destatis 1+2N (info+tables per statistic), boj `getMetadata` per DB, sdmx `series_keys` per flow | Yes, within the crawled roots |
| C | **Hardcoded / curated registry** | transcribe a list the provider publishes only as a doc/nav-tree | boj `_BOJ_DATABASES` (50), bde 7 CSV chapters (snb *was* here — `_KNOWN_CUBES` (237) — until 2026-06-09, now archetype A via the sitemap) | **No** — a liability; see guards |
| D | **Hybrid (live + static merge)** | live registry for the bulk, hardcode the out-of-band families | treasury (live `/dtg/metadata` measures **+** hardcoded ODM yield-curve feeds), riksbank (live SWEA `/Series` + live Monetary Policy `/series_ids` **+** static SWESTR/Turnover/Holdings registries — A-per-product unioned into one catalog) | Partly |
| E | **Scrape publication indexes** | regex link patterns + fixed-shape metadata header rows | rba (3-pass HTML scrape: CSV index, **dynamic-exclusivity** XLSX, legacy `.xls`) | No — re-templating breaks it |
| F | **Brute-force Cartesian enumeration** | last resort when no series-keys stream exists | sdmx WB (`path × decade` sweep, 404 = empty) | No — a **lower bound**, not exhaustive |

**Prefer A.** A live full-index endpoint means the catalog self-tracks provider additions and
you can verify completeness by diffing `len(catalog)` against the live endpoint's count. Only
fall to C/E/F when the provider genuinely exposes no machine-readable listing.

**The hardcoded-registry liability (archetype C/D's static part).** A frozen list rots
silently when the provider adds data. Mandatory guards:
- **Commit the reproduction script.** SNB's registry comment *used to* cite a `discover_cubes.py`
  that **did not exist** in the repo — no committed way to regenerate the 237-cube list (the
  canonical cautionary case). Resolved 2026-06-09: SNB now discovers its universe **live** from
  the published sitemap (archetype A), and ships `scripts/harvest_cubes.py` (the long-promised
  reproduction script, with a `--diff`). The lesson stands for any freeze you keep: don't cite a
  script you didn't commit; prefer a live index when the provider publishes one (it usually does —
  a sitemap, a flat export — even when the docs point you at a walled nav tree).
- **Floor + shape tests.** A `len(_REGISTRY) >= N` floor test catches *shrinkage* (not
  incompleteness); a shape test forbids malformed ids. BoJ pins exactly 50 DBs and asserts the
  phantom `BP02` is absent (its list drifted before: 45 + phantom → canonical 50) — **and
  cross-validates the freeze against BoJ's own machine-readable `api_tool.xlsx` `DB_Name` sheet
  via a committed `harvest_databases.py` (`--diff` = zero drift)**, the SNB-cautionary fix done
  right: a frozen registry is only safe if a re-derivation script proves it still matches source.
- **Cross-validate.** BoJ's 50 DBs were transcribed from the official API-manual PDF and
  cross-checked against two third-party libraries. BdE found two missing chapters (`cf`, `ie`)
  via a brute-force `catalogo_{aa..zz}.csv` probe after shipping with 5.
- **Document that a human must re-sync on provider changes**, and reserve a `source=` tag for
  out-of-band families so they slot in cleanly later. **But never trust an old connector's
  "this family 404s / isn't fetchable" comment** — riksbank's code claimed "`forecasts` 404s on
  every path" and excluded the entire Monetary Policy product; it had simply probed the wrong
  base URL (`forecasts/v1` vs the real `monetary_policy_data/v1/forecasts`). The authoritative
  cross-check is the provider's own **product list** (a dev-portal `/apis` page or its backing
  management API), not the predecessor's assumptions — that's how the riksbank re-run found it
  covered only 2 of 5 public APIs.

**Best-effort vs fatal.** Enumeration fan-outs should be **best-effort**: per-item probe/
membership failures are logged-and-skipped (providers leave retired ids in their indexes) so a
partial catalog beats none. **But** a failing *list/data* endpoint must raise a typed
`ProviderError` — enumeration tolerance must not become silent data loss on the fetch path.
The hazard: a transient 5xx during a publish run silently shrinks the snapshot. So **always log
a `failed/total` summary** (`"M/N statistics failed metadata fetch"`, BoJ's `failed_dbs`,
bdf's `failed_datasets`) and **diff emitted-row counts across publish runs** to catch a
quietly-shrunk catalog before shipping it.

### 7.3 Creative catalog techniques

The catalog is a *designed* artifact. Techniques drawn from the existing connectors:

- **Compound codes** when one fetchable endpoint exposes many measures, or ids are reused
  across tables:
  - `{endpoint}#{field}` — treasury (`v2/accounting/od/debt_to_penny#…`, `home/<feed>#…`).
  - `{table_id}#{series_id}` — rba (series ids are reused across `B13.1.x`/`B13.2.x`; a bare id
    KEY would dedup ~5% of entries). snb mirrors this (`{cube}#{series_key}`).
  - `{agency}|{dataset_id}` — sdmx datasets (`ECB|YC`).
  - Use a compound KEY **only** when the provider reuses ids, **or** when one catalog spans
    several products whose fetch verbs differ and a hit must route by code shape. riksbank is the
    latter: SWEA/SWESTR keep bare ids (self-routing — disjoint id spaces), but the three other
    products carry a routing prefix (`monetary_policy/<id>`, `turnover/<market>/<freq>`,
    `holdings/<dataset>`) because the search verb can only return `code`/`title`/`score`, so the
    code is the *only* thing that can route the follow-up fetch — and bare MP ids share SWEA's
    `SED*`/`SEM*`/`SEA*` prefix space, so a bare id would be ambiguous. Decide per-provider from
    the actual id namespace **and** the number of distinct fetch verbs the catalog feeds.
- **Synthetic parent rows** so the catalog carries hierarchy: `db:<code>` (boj), `group:<NAME>`
  (boc), `dataset:<id>` / `domain:<id>` (bdp). Agents navigate by KEY-prefix or an
  `entity_type` METADATA column. Catalog the granularity the agent should *search* at, even if
  coarser than the leaf series — boc catalogs **groups** as first-class entities because
  `boc_fetch` accepts whole-panel `group:NAME` queries and group-level descriptions
  (units/frequency) carry retrieval signal no individual series has.
- **`source=` dispatch metadata** so one search catalog routes to multiple fetch connectors:
  treasury (`fiscal_data`→`treasury_fetch`, `treasury_rates`→`treasury_rates_fetch`), riksbank
  (`swea`/`swestr`), bde (`bde_biest`). Spell the routing out in the **search connector's
  description** too so the agent never has to sniff a code prefix.
- **Dimension manifests for two-stage search** (sdmx): derive a compact `{id, values:[{code,
  label}]}` manifest (capped ~12 values/dim) from each built series bundle and attach it to the
  parent **datasets** bundle, so a coarse `datasets_search` hit tells the agent which
  structured `FIELD:value` fields the next `series_search` accepts. The catalog
  self-documents its own query vocabulary (`datasets_search` → read dimensions →
  `series_search` → `fetch`).
- **Multi-bundle sharding** for huge providers: one bundle per agency for discovery + one per
  `(agency, flow)` / per database for depth (sdmx → thousands of bundles; boj → per-DB). Keeps
  each namespace tractable and embedding memory bounded. `catalog_root` + per-probe
  `namespace` lets one `queries.yaml` validate the whole fan-out.
- **Per-row namespaces** (`namespace="__row__"` + an `entity_namespace` METADATA column) when
  one enumerator spans several entity families.
- **Cardinality discipline.** Collapse high-cardinality cartesian products to a coarser
  discoverable entity rather than emitting every redundant crossing: snb collapses ~9
  mega-cubes (>100 dimension-leaf crossings) to a single `{cube}#` row to keep the catalog
  under ~5k rows and not drown the embedder (the series stay fetchable via `dim_sel`).
- **Description synthesis & lifting.** Never let a missing attribute create an unsearchable
  row: synthesize a description (`<id> — published by <provider>`) and tag a
  `frequency_source` (`group`/`suffix`/`registry`/`unknown`) so consumers can tell a confident
  value from a heuristic (riksbank). Lift a parent's long description onto thin child/table
  rows (destatis) because leaf titles are too short to embed well. ECB spends a side-channel
  call for short semantic `TITLE`/`TITLE_COMPL`; other agencies fall back to deterministic
  codelist-label concatenation — but **don't duplicate both** (O(N²) embedder cost), and always
  keep the raw key in a `code` column so keyword-exact queries survive any title choice.
- **Distinct entity classes get distinct namespaces** — never share (`polymarket_market` vs
  `polymarket_event`).

### 7.4 Index policy (searchability)

`discovery_indexes(entries)` (from `parsimony.catalog.policy`) gives: `code` → `BM25Index`
(exact lookup), `title` and `description` → `adaptive_field_index`. **Adaptive returns a
Hybrid BM25+vector index only when the field has < 1000 unique values, else BM25-only**
(weights BM25 0.5 / vector 1.0 via `ZScoreFusion`). `default_field="title"` enables broad
(plain-text) search; structured `FIELD: value` (AND across `&&`, OR across `,`) works on any
indexed field. **Know the 1000-unique threshold** — a high-cardinality `title` silently
degrades to BM25-only, so a semantic-style title probe will miss. SDMX uses a custom policy
that additionally indexes one adaptive field per observed SDMX dimension. Put **every
searchable attribute as a METADATA column** so the policy can index it.

### 7.5 Snapshots & the cache

A built catalog persists to a portable snapshot: `entries.parquet` (zstd) + `indexes/<field>/`
+ `meta.json` (with a `content_sha256` integrity digest verified on load). URL schemes:
`file://` (or a bare path) and `hf://<org>/<repo>[/<sub>]` (a HF **dataset** repo). Publish to
`hf://parsimony-dev/<provider>` so `<p>_search` doesn't cold-build. Catalogs cache under
`~/.cache/parsimony/catalogs/<provider>/<namespace>/` (override the whole root with
`PARSIMONY_CACHE_DIR`); inspect with `uv run parsimony cache info`. `schema_version` is a hard
gate — rebuild + re-push whenever the kernel `SCHEMA_VERSION` bumps.

### 7.6 The two completeness questions, answered operationally

**Q1 — does the catalog contain ALL series?**
- Identify the authoritative enumeration path (§1.4 of the dossier) and **diff the catalog
  entry count against it**: boc vs live `/lists/series` count; destatis vs ~331 statistics +
  the `M/N failed` summary being 0; sdmx vs `len(client.dataflow(force=True).dataflow)` minus
  `$`-flows; treasury vs (879 measure fields) + 35 ODM rows (registry == live 2025 columns); bdf/bdp vs their logged
  discovered/emitted counts.
- For frozen registries, a floor test + a manual re-harvest diff is the only signal.
- The `queries.yaml` **recall gate** (§9) proves known series are *findable*, not just present.

**Q2 — do the connectors cover ALL accessible data?**
- Enumerate the provider's documented endpoint families and check each is reachable. The honest
  answer is usually **PARTIAL by design** — document the deliberate exclusions in the README
  and the dossier so the gap is a *choice*, not an accident. Common legitimate exclusions:
  premium/plan-gated endpoints, streaming/websockets, binary formats, and endpoints a sibling
  connector covers better (alpha_vantage defers commodities to FRED). **But distinguish
  *inaccessible* from merely *unimplemented* — close the latter.** rba catalogued XLSX-exclusive
  + xls-hist series its CSV-only fetch couldn't reach; the 2026-06-09 re-run made `rba_fetch`
  resolve all three publication formats so every catalogued series is fetchable (use the
  `source=` dispatch column to route a hit to the right format). A catalogued-but-unfetchable
  row is a gap to fix, not a permanent exclusion, whenever the data is reachable.
- **Live-verify** every verb (§8) — the only proof that a wrapped endpoint actually returns
  real data.

---

## 8. The completeness mandate (live verification)

**Documentation is a claim; execution is the truth.** A connector can pass conformance and
every offline test while N verbs 404/422 on the real API or return empty/constant columns. The
strongest evidence in the whole sweep: `eodhd` shipped with 5 ship-breakers (wrong routes:
`eod-bulk-last-day`, `technical`, `ipos`, lowercase search type, indicator-vs-topic) behind a
green conformance+offline gate, because only 1 of 17 verbs had a live test.

**The rules:**

1. **Live-E2E every verb, asserting REAL content** — `df["value"].notna().any()`,
   `df["description"].str.len().gt(0).any()` — not just that the columns exist. Column-name
   checks let empty/constant columns pass.
2. **Confirm the upstream payload actually populates a field before declaring a column for it.**
   Drop dead columns (EIA `frequency`, constant `category`). A declared-but-always-empty column
   is dishonest about what data is accessible. (Distinguish "field absent from payload" — drop
   it — from "field present but empty in this slice" — keep, maybe `exclude_from_llm_view`;
   finnhub `isin` is empty on the US default but real for other exchanges.)
3. **For facet/dimension APIs, expose the measure/facet id as a parameter** with a sensible
   default and normalize the output to a stable column name, then **live-test at least one route
   whose facet differs from the default**. EIA's `/data` returns metadata-only (no values)
   unless `data[0]=<measure>`, and the measure is route-specific (`value` for petroleum,
   `price`/`sales`/`revenue`/`customers` for electricity) — hardcoding `value` 400s on
   electricity, and the old test "passed" on non-empty + title while returning no values.
4. **Live-probe each frequency/route/mode variant** — they can hit different endpoints with
   different field names, and a shared schema silently drops the mismatched columns. fmp
   `frequency="dividend_adjusted"` returns `adjOpen/adjHigh/adjLow/adjClose` (not
   `open/high/low/close`); rename **before** projection or the output collapses to `[date,
   volume]`.
5. **When live tests SKIP for a missing credential, the connector is UNVERIFIED and can ship
   broken** (sec_edgar + eia both did). The reviewer must do a read-only **out-of-band probe
   with a throwaway credential** (SEC accepts any non-empty UA at low volume) to confirm
   host/path/shape. Lock the host with a respx test that asserts the GET hit the right *host*,
   not just path.
6. **Honest flagging beats false green.** Quota/plan/credential-blocked verbs use a
   content-OR-(`RateLimit`|`PaymentRequired`) helper and a ⚠️ flag, **never** a silent skip or
   mock-pass. On quota-limited providers (alpha_vantage = 25 req/**day** shared across all
   endpoints), schedule a fresh-quota-day pass to reach the unprobed verbs.

**Bounded live tests** (verify the live API shape without firing a full crawl):
- Expose the universe source as a **module-level seam** read at call time (`_list_datasets`,
  `_list_domains`, `_list_groups`, `_KNOWN_CUBES`, `_load_statistics_index`, `_list_databases`,
  `CATALOG_CHAPTERS`) and **monkeypatch it to 1–3 entities**.
- Wrap `HttpClient.request` with a **counter** that asserts the fan-out stayed a handful (`<10`
  vs ~2,400; `<25` for one bdp domain).
- For `*_search` tests, build a tiny **fixture `Catalog` in `tmp_path`** and pass
  `catalog_url=str(tmp_path)` — network-free, never a cold full build. Assert that a *second*
  different query surfaces a *different* top hit (proves ranking discriminates, not just
  returns row 0).

**The recall gate (`catalog_tests/queries.yaml`).** Curate probes per provider:
`required` exact-`code:` probes + short lexical `title_bm25` probes, `optional`
`hybrid_title`/`structured_field` probes (long/semantic title probes must be `optional` because
the index degrades to BM25-only above 1000 unique values), ending with
`thresholds: {min_required_recall: 1.0}`. `report.ok = schema_ok AND every required probe's
expected_code appears in catalog.search(query) top results`. Auto-draft a starting set with
`validate_catalog.py --write-queries` (samples real entries, emits one probe per indexed field),
then hand-curate. This is how you prove the catalog covers the data an agent will actually
search for.

---

## 9. Testing

Per-package `tests/`:

- **`test_conformance.py`** — `assert_plugin_valid(parsimony_<p>)`. Release-blocking.
- **`test_<p>_connectors.py`** — offline, `respx`-mocked (hand-authored from upstream docs; **no
  recorded cassettes**; `fixtures/**` is gitignored). Per-verb happy path + `EmptyData`/`Parse`/
  `InvalidParameter` guards + the provider-specific status mapping + a parametrized **no-key
  fast-fail over ALL verbs** with a **count-guard** test (so dropping a verb breaks CI) +
  secret-stripping. Assert only on the public `Result` surface (columns, `provenance.source`),
  never httpx internals / full-DataFrame equality / timing.
- **`test_error_mapping_<p>.py`** — subclass `ErrorMappingSuite` from `parsimony_test_support`
  (set `connector`, `route_url`, `method`, `env_key`/`env_value` or `env_key=None` for keyless,
  `provider`). It injects a `CANARY_KEY` and asserts it never appears in a `ConnectorError`
  message, `provenance`, or `to_llm()`, plus the canonical status table and the `Retry-After`
  contract. For non-canonical statuses, add bespoke 401-vs-403 assertions; for non-httpx
  transports (rba/curl_cffi) pin the hand-written mapper directly (the suite doesn't apply).
- **`test_public_surface.py`** — `__all__`, exact `CONNECTORS` count, internal symbols not
  re-exported.
- **`test_integration_<p>.py`** — `@pytest.mark.integration` (excluded by default via
  `-m 'not integration'`); every verb live, real content, `assert_no_secret_leak` with a real
  key; bounded enumerate + fixture-catalog search.
- Catalog packages also: **`catalog_tests/queries.yaml`** (recall probes) and
  **`test_build_catalog.py`** (index types + `default_field`).

`test_support/` holds the shared `ErrorMappingSuite`, `CANARY_KEY`, `assert_no_secret_leak`,
`assert_provenance_shape`, `require_env`.

---

## 10. Packaging, registration, release

- **Three names in lockstep** (PR checklist enforces): PyPI `parsimony-<name>` (hyphen),
  import `parsimony_<name>` (underscore), directory `packages/<name>/`.
- **`pyproject.toml`**: `name = "parsimony-<name>"`, `license = "Apache-2.0"`,
  `requires-python = ">=3.11"`, `dependencies = ["parsimony-core>=0.7,<0.8"]` (a
  **contract-version pin**, not a floor — any lingering `>=0.6,<0.7` in README/CONTRIBUTING is
  **stale**), `[project.urls] Homepage = …`, hatchling
  `[tool.hatch.build.targets.wheel] packages = ["parsimony_<name>"]`, pytest `integration`
  marker, ruff per-file `E402` ignore for `scripts/*`.
- **Entry point** (the only registration): `[project.entry-points."parsimony.providers"]`
  `<provider> = "parsimony_<name>"` (**bare module path** — verified against `discover.py`;
  the `:CONNECTORS` form is stale). CI's `discover` job enforces the stanza exists. Discovery
  is entirely via installed entry-point metadata — **no central registry/index file**.
- **`CONNECTORS = Connectors([...])`** at module top level — the discovered surface (all fetch
  + enumerator + search). Catalog-build workflows live in `scripts/`, never the user-facing
  module. No `__version__`/metadata dict (discovery reads from `importlib.metadata`).
- **Per-package release**: own version, own `release.yml` trigger, own PyPI OIDC Trusted
  Publisher — independent cadence. Releases go bottom-up after `parsimony-core`. Refresh the
  README roster with `make readme-roster` (`scripts/gen_roster.py` sweeps `packages/*/
  pyproject.toml` and counts `@connector` decorations).
- **Catalog-validate registry**: add a `ProviderCatalogSpec` to `PROVIDER_SPECS` in
  `tooling/catalog_validate/registry.py` (`provider`, `default_url`, `build_script`,
  `search_mode`, `queries_file`) — **or** to `EXCLUDED_COMMERCIAL_PROVIDERS`
  (`{alpha_vantage, coingecko, eodhd, finnhub, fmp, fred, polymarket, sec_edgar, tiingo}`) if it
  has first-party search. The `catalog_validate` package is maintainer-only (in `tooling/`, not
  shipped, not part of the contract).
- **The local gate (mirrors CI exactly):** `make verify PKG=<name>` = ruff + **full-package
  mypy (including `tests/`)** + pytest + `parsimony list --strict`. "If it passes locally, CI
  passes." `make verify-all` across packages. Restore the toolchain after a bare `uv sync`
  (which prunes ruff/mypy) with `uv sync --all-extras --all-packages`, and confirm
  `uv run ruff --version`.
- **Operator catalog workflow:** `build_catalog.py --save file:///tmp/…` → `validate_catalog.py
  --provider <p> --catalog-url file:///tmp/…` (expect `overall: OK`, `required_recall 1.00`) →
  `build_catalog.py --push hf://parsimony-dev/<p>` (needs `HF_TOKEN`) → re-validate the `hf://`
  URL. Use `--allow-missing-remote` only in CI.
- **Cross-repo:** a key sent as a GET query param under a non-standard name needs its name added
  to `parsimony-core`'s `_SENSITIVE_QUERY_PARAM_NAMES` — a parsimony-core PR dependency at land
  time (BLS `registrationkey`).

---

## 11. Connector roster & archetype index

Use this to find the closest existing exemplar to copy when starting a new provider. (Counts &
notes from the 0.7 survey; verify against current source.)

### Commercial / native-search (NO catalog — wrap the provider's search)

| Provider | Auth | Discovery | Copy it for… |
|---|---|---|---|
| **alpha_vantage** (29) | `apikey` query, required | native `SYMBOL_SEARCH` | 200-with-error-body; multiplexing many "functions" behind one verb (econ=10, technical=52) via an allow-list; CSV+JSON paths; synthetic KEY injection |
| **fmp** (19) | `apikey` query, required | native search + screener | status-only plan mapping (402/403→Payment); a pooled+semaphore screener fan-out with a zero-enrichment short-circuit and agent-correctable error text |
| **coingecko** (11) | `x-cg-demo-api-key` header | native `/search` | 401 dual-meaning by numeric `error_code`; one client serving two product APIs via a path prefix |
| **finnhub** (12) | `token` query | native `/search` | 403→Payment status-only; a 302→CDN enumerator |
| **eodhd** (17) | `api_token` query | native search | 403/**423**→Payment; `_select_declared` projection with `*` opt-out; per-exchange (not global) universe |
| **tiingo** (13) | `Authorization: Token` header | native search | 403 **body-sniff**; a dedicated second client for an unauthenticated CDN `supported_tickers.zip` enumerator (~127k) |
| **fred** (2) | `api_key` query, required | native `/series/search` | the reference "native search ⇒ delete the enumerators / no catalog" case |
| **polymarket** (3) | none | native Gamma `/markets`,`/events` | pure no-auth baseline |
| **sec_edgar** (7) | UA header (required, non-secret) | native full-text search (`efts`) + `company_tickers.json` | keyless-but-header-required; **three hosts** (`efts`/`data`/`www`); the native-search wrapper that justifies the no-catalog classification (full-text over all filers' content, 2001→); **four atomic units, not a timeseries** (registrant/filing/document/XBRL-fact); `index.json` primary-doc resolution (any-age, dodges the XSL-viewer path); submissions `filings.files[]` page-walk; XBRL in three aggregations (per-company concept history, all-facts, cross-company `frames`); FTS `dateRange=custom` 500-gotcha + **relevance-ranked, no date sort** (probed: `sort=date`→500, others ignored — document it, don't fake a client-side sort); non-JSON body via `_get_text` |

Middle ground (keyed/optional, shallow enumerator, no full catalog): *(none left — both
former residents graduated to full catalogs).* **bls** lived here (its `timeseries/popular`
enumerator was the cautionary shallow case; refactored 2026-06-09 to a two-tier catalog) and
**eia** lived here (its top-level-routes-only enumerator; refactored 2026-06-09 to a full
route-tree dataset catalog + the out-of-tree `seriesid` fetch path). Both now in the
built-catalog table + §13.

### Public / built-catalog (the completeness path)

| Provider | Auth | Enum archetype (§7.2) | Copy it for… |
|---|---|---|---|
| **treasury** (4) | keyless | **A+D** (Fiscal Data live `/dtg/metadata` = A self-tracking; ODM 5-feed registry = D) | compound `{endpoint}#{field}`; merging famous series that aren't in the metadata API; JSON+XML in one package; `_get_text`; the **name-the-prose-column-`description`-or-it-isn't-indexed** trap (a `definition` column meant the catalog searched `title` only); the **archetype-D harvester** (`harvest_rate_feeds.py`) that proved the ODM registry == the live 2025 column union exactly (incl. the 1.5-month + 6-week tenors Treasury added in 2025); the live cross-check that **refuted a grep-based "phantom" hypothesis of mine** (OData omits null props per-entry, so a sparse new tenor is invisible in a first-entry read) |
| **bde** (3) | keyless | **C** 7 hardcoded CSV chapters | CP1252 CSV crawl; `/`-path title splitting; best-effort degrade |
| **bdf** (3) | `Apikey` header, required | **A** flat `series` full-index export (~41.6k) | Opendatasoft `Apikey` scheme; the 17-system-vs-45-dataflow count trap; bilingual + breadcrumb catalog for free |
| **bdp** (3) | keyless (Akamai) | **B** domains→**paginated** datasets→**paginated** series (`page_size=100&obs_last_n=1`); `/series/` EN+PT enrichment | JSON-stat; two pagination levels (both followed); per-dataset `num_series` self-check; WAF survival headers |
| **boc** (3) | keyless | **A** live `/lists/series` full index | the gold-standard self-tracking catalog; groups as first-class entities; **fan-out-as-liveness-probe** (prune 404 groups); **URL-length fetch guard** (~4 KB request-URI cap); completeness proven by diffing the group fan-out against the master list (0 leak) |
| **snb** (3) | keyless | **A** live sitemap (`/sitemap`, 1,149 cubes) | the cautionary frozen-registry **fixed** (archetype C→A: live sitemap + committed `harvest_cubes.py`); **two cube families in one provider** — 237 publication (`/api/cube/...`) + **912 data-warehouse** (`/api/warehouse/cube/{@→.}/...`, the SDMX store the old connector excluded) routed by one `snb_fetch`; the **`x-epb-ajax` header WAF unlock** for the portal's `/json` metadata API (cube titles via `getCubeInfo`, best-effort); mega-cube + warehouse cardinality collapse; CSV `_get_text` |
| **rba** (3) | keyless (Akamai) | **E** 3-pass HTML scrape | **curl_cffi** impersonation; the no-API archetype-E exemplar (CSV index + current-XLSX-exclusive sheets + legacy `.xls`, scraped from 2 publication-index pages); **dynamic XLSX exclusivity** (emit a workbook series only if not in the CSV-covered set — self-maintaining, replaced a hardcoded allow-list); one `rba_fetch` resolving a `table_id` across all 3 formats so every catalogued series is fetchable; `readrba` (R client) as the canonical reference impl + completeness cross-check |
| **riksbank** (7) | optional key (header) | **A-per-product unioned** (live SWEA `/Series` + live Monetary Policy `/series_ids`; static SWESTR/Turnover/Holdings registries) | the **whole-API treasury-trap**: a deep portal survey found the connector covered only **2 of 5** public Riksbank APIs — adding Monetary Policy (forecasts × policy-round vintages), Turnover (market×freq faceted JSON), Holdings (parquet-advertised but JSON-served, so no pyarrow); **never trust an old "this family 404s" comment** (the wrong base URL had it excluding the entire forecasts product); cross-check coverage against the **dev-portal `/developer/apis` product list** (read via DevTools); **routing-prefix codes** when one catalog feeds several fetch verbs (search returns only `code`); the **literal-colon query fix** (`policy_round_name=2026:1` — httpx `%3A`-encodes it → gateway 404 → silent whole-universe fallback; `safe=":"` raw helper) |
| **destatis** (3) | keyless (browser UA) | **A→B** `/statistics` (331) + 1+2N `/tables` fan-out (**3,009 tables, lossless** — full lists, no pagination cap, 0 cross-stat dups) | 200-HTML-error-body; JSON-stat; **time axis by key-SHAPE not name** (`STAG`/`SEMEST`/`SMONAT` keys *are* the periods; `MONAT`/`QUARTG` are month-/quarter-of-year *classifications* — and never fall back to dim 0 = the constant `statistic` code → bogus "year" ParseError on ~25% of tables); tableless-statistic (404 `/tables`) keeps its row; predefined-tables-only (cubes absent on the keyless host); parent-description lift |
| **boj** (4) | keyless (Akamai) | **C+B** frozen 50-DB registry (XLSX-cross-validated + committed harvester) + live `getMetadata` per DB (uncapped) | **multi-bundle** two-step search over a **326k-series** universe (CO/TANKAN alone 166k); the **HTTP-200 + `NEXTPOSITION` fetch-truncation** trap (paginate it, don't trust "completed"); breadcrumb reconstruction from interleaved headers |
| **eia** (5) | `api_key` query, required | **B** route-tree walk (`/v2/` → recurse `routes` → 232 leaf datasets, inline child lists, no list-pagination) | keyed **two-tier** (datasets catalogued; ~2M-series universe fetch-only); the **5,000-row `/data` cap → silent truncation** fix (read `response.total`, page by `offset`, **NO sort** — sort introduces a boundary gap+dup); the **`/v2/seriesid/{id}` out-of-tree legacy path** (only the *docs* revealed it — a tree walk can't); facet-manifest catalog rows + `eia_facets` narrowing; measure-col detection by `{col}-units` sibling; row-count ceiling with EIA's own narrow-guidance |
| **sdmx** (5) | keyless | **A+B+F** per-agency dataflow + per-flow series-keys + WB brute-force | the hardest completeness story: subprocess isolation (leaky `sdmx1`), dynamic per-DSD schema (→ `@connector` not `@enumerator`), `$DV_*` skip, dimension manifests, 4 of 7 advertised agencies wired |
| **bls** (5) | optional key (POST body); flat files Akamai | **two-tier** (sdmx-shaped): survey index + per-survey `.series` | uncatalogable universe (~tens of M series / 15.6 GB) → catalog surveys + dimension vocab completely, per-survey series for headline surveys, **everything fetchable by id**; curl_cffi for the flat-file host, plain httpx for the API; dynamic per-survey schema; title composition for title-less surveys |

---

## 12. Cross-cutting gotchas

The traps that bite regardless of provider:

- **Typed-error arg order is flipped** vs the base — `provider` is first positional on every
  subclass (§4.5).
- **200-with-error-body** → `RateLimitError(quota_exhausted=True)` or `ParseError`, never a fake
  `status_code=0`/`ProviderError` (§6.4).
- **Blanket `to_numeric` NaNs string metadata** — coerce only the measure column; all-NaN →
  `ParseError`, so pick real-data windows live (§6.6).
- **Query-param key redaction only covers known names** — a custom-named query key leaks to logs
  (§4.6).
- **Present-but-invalid key is often not 401** (FRED → 400) (§5.5).
- **Enumerators require an EXACT column match and silently drop unmapped columns** — declare
  every column and `return df[DECLARED].head(limit)`; plain `@connector`/`@loader` fold unmapped
  columns in as DATA, so `return df[[declared]]` to drop passthrough (§4.2).
- **`Connectors` is name-keyed** — `bundle[0]` raises `KeyError`; merge with `+`.
- **`CatalogNotFoundError` and all `parsimony.transport.*` are not top-level** — import from the
  submodule.
- **Adaptive index degrades to BM25-only above 1000 unique values** — a "semantic" title probe
  marked `required` will then MISS (§7.4, §8).
- **Multi-bundle catalogs aren't one flat catalog** — set `catalog_root` + per-probe
  `namespace`; a required probe against an unbuilt bundle hard-fails (§8).
- **`schema_version` mismatch is a hard gate** — rebuild+re-push on a kernel `SCHEMA_VERSION`
  bump; `schema_ok=False` means "stale snapshot," not "wrong catalog."
- **Operational toolchain traps:** a bare `uv sync` (e.g. after a version bump) **prunes
  ruff/mypy** from the venv (restore with `uv sync --all-extras --all-packages`); the worktree
  **direnv cache can predate** a newly-added `.env` key, silently SKIPping live tests (source
  `.env` explicitly: `set -a; . /home/espinet/ockham/.env; set +a; uv run pytest … -m
  integration`); **never echo a key value**; gate **full-package** mypy (including `tests/`).
- **Doc drift is real** — README/CHANGELOG in several packages still say
  `parsimony-core>=0.6,<0.7` and "connectors only / no catalog" while shipping 0.7 + a catalog;
  CONTRIBUTING shows the stale `:CONNECTORS` entry-point form. **Verify against source, not the
  prose** — exactly the discipline this guidebook exists to enforce.

---

## 13. Running findings log

Append dated, transferable findings here as new providers are tackled. When a finding changes a
rule, also edit the relevant section above.

- **2026-06-08 — Guidebook seeded.** From a first-hand read of `parsimony-core` (source +
  `parsimony/docs/`), all 24 existing connector packages, and the 0.7 sweep dossiers
  (`_planning/connectors-catchup/{LESSONS,AUTHORING-CONTRACT-0.7}.md`). Verified against
  `discover.py` that the entry-point value is a bare module path (the `:CONNECTORS` form in
  `CONTRIBUTING.md` is stale) and that the current contract pin is `>=0.7,<0.8` (README/
  CONTRIBUTING `>=0.6,<0.7` is stale). No new provider tackled yet — that is the next task.

- **2026-06-08 — BdE re-run through the full process (dossier: `providers/bde.md`).** A "rushed"
  0.7-sweep connector that was code-clean but **never completeness-verified**. Transferable
  lessons:
  - **Code-clean ≠ complete.** The 0.7 sweep fixed transport/params/tests but never asked the
    two completeness questions. A reconciliation that explicitly waives "Phase 2 must NOT run a
    full enumerate live" is a flag that completeness was never checked. **Re-run the
    completeness questions even on connectors that already "pass."**
  - **The enumeration source can lie about fetchability — verify with an exhaustive live sweep,
    not a sample.** BdE's own docs admit "the web service only includes a subset." A
    25-code/chapter sample said "~81% fetchable"; the full 15,547-code `favoritas` sweep gave
    the real 97.6% and the *exact* un-fetchable set. Cheap availability endpoints (here
    `favoritas`, which returns `serie:null` placeholders instead of erroring the batch) are the
    right tool for a whole-universe fetchability audit.
  - **Dedup the enumeration.** Cross-source repeats are common (24% here — a series listed under
    its home chapter *and* a summary chapter). Without dedup-by-key the catalog is bloated and
    per-row metadata (category) is non-deterministic by crawl order.
  - **An un-fetchable "registry/family" code may be an *alias* with the real code hidden in a
    bulk file.** BdE's `pb` CSV listed un-fetchable `PB_1_1.1` family aliases; the real
    fetchable `DPB…` codes lived only in the bulk `pb.zip` (a transposed value file: "NOMBRE"
    row = real codes, "ALIAS" row = the catalog code). **Before excluding an un-fetchable slice,
    look for a bulk/ZIP download that carries the real codes** — recovered ~350 series here.
    (Watch for mixed layouts in one archive: `pb.zip` also bundled the alias `catalogo_pb.csv`;
    filter members + guard against whitespace "codes".)
  - **Range/pagination params can be frequency-dependent.** BdE's `rango` differs by frequency
    (daily rejects `MAX`, takes `3M/12M/36M`; monthly takes `30M/60M/MAX`). A single hardcoded
    valid-set is wrong for a multi-frequency provider — accept the documented union and let the
    provider validate, mapping its rejection to `InvalidParameterError`.
  - **Map a provider's "bad input" status to `InvalidParameterError`, preserving its message.**
    `fetch_json` collapses the body, so for a provider that returns a useful error payload
    (BdE's `errMsgDebug` at HTTP 412) drop to `HttpClient` + a local `except HTTPStatusError`
    that reads the body, rather than letting the generic 4xx→`ProviderError` mapping swallow it.
  - **Don't wrap an endpoint that adds no unique *data*.** A field-level diff showed `favoritas`
    = `listaSeries`'s latest value + a derived trend arrow. "Cover all accessible data" means
    data classes, not endpoints — documenting *why* it's skipped is the deliverable.
  - **Spanish-only (or any single-language) catalog + a BM25 index = no cross-language recall.**
    The adaptive index degrades to lexical BM25 above ~1000 unique values, so there is NO
    multilingual embedding bridge. If the provider serves titles in the user's language through
    a *separate* endpoint, enrich at build time. For BdE the cheap path is `favoritas(idioma=en)`
    (latest value only, no history) — NOT `listaSeries(idioma=en)` (full history per series).
    Keep the original-language text in `description` (also indexed) for bilingual recall.
  - **A best-effort batched enrichment must retry + split, or it silently under-fills.** A flaky
    window made one `favoritas` build drop ~82% of English titles with a single logged warning
    (well-known codes that *have* English came back without it). The fix: per-batch retry with
    backoff, then split-the-batch-and-recurse on persistent failure (release the concurrency
    semaphore *before* recursing, or you deadlock). "Logged and skipped the whole batch" is a
    silent-data-loss bug dressed as resilience.
  - **Heavy live probing can get your IP silently blocked — and there's no error message to
    read.** After an exhaustive 15.5k-code sweep + repeated full catalog builds, BdE began
    timing out at the **TCP connect layer** (`Trying <ip>:443… Connection timed out`) on *both*
    hosts (API + static CSV, same /24), while the rest of the internet resolved and responded
    fine. Diagnose the failure layer before assuming: a documented/app-level rate limit
    completes the TLS handshake and returns **HTTP 429 + `Retry-After`**; a silent packet-drop
    before any HTTP response is an **edge/firewall IP block** — no status, no body, no header,
    by design. BdE documents no rate limit, so there is nothing to consult; the *absence* of an
    error is the signal. Budget live verification accordingly: sample to answer the question,
    cache downloads and re-parse offline, verify built artifacts from disk. A 0-entry
    "best-effort" build means the source went dark, not that the catalog is empty.
  - **Verify the built artifact offline.** Dedup, key-uniqueness, recovered-code presence, and
    no-leaked-aliases are all checkable by reading the saved `entries.parquet` with zero network
    — do that before trusting (or publishing) a snapshot. Coverage of a *live-enriched* field,
    though, needs a clean-network build to certify; a snapshot built during a flake is stale.

- **2026-06-08 — BdF refactored from scratch, now verified-live (dossier: `providers/bdf.md`).**
  The "cautionary `UNVERIFIED-LIVE`" case turned out to be a missing-key *naming* problem, not a
  missing key: a working Webstat key sat in `ockham/.env` as `BANQUEDEFRANCE_KEY`, not the
  convention `BDF_API_KEY`. Transferable lessons:
  - **Before declaring a connector unverifiable for lack of a key, grep `.env` for the
    provider under *any* name.** The whole "UNVERIFIED" status rested on the key being absent;
    it was present under a non-convention name. Mirror it to `<PROVIDER>_API_KEY` (ask before
    editing the user's `.env`) and the connector is instantly verifiable. (Watch the in-place
    edit: a careless rewrite *replaced* `BANQUEDEFRANCE_KEY` instead of adding alongside —
    back up `.env` and re-read it after writing.)
  - **A "catalog count" is not always your universe count.** Webstat is Opendatasoft Explore:
    `GET /catalog/datasets` returns **17** (the ODS *system* datasets), while the real BdF
    dataflow list (45) lives *inside* the `webstat-datasets` table and the series universe
    (41,641) inside the `series` table. The old code's "45 datasets → per-dataset crawl"
    mistook the dataflow layer for the enumeration unit.
  - **A flat "everything" table turns archetype B into archetype A.** Because ODS exposes
    `series` as one queryable flat table, the entire universe streams from a single
    `series/exports/json` (with a lean `select=` — the table is ~200 sparse columns) instead of
    a 45-call per-dataflow fan-out. Prefer the single full-index export: it self-tracks
    additions and `len(catalog)` diffs cleanly against the live `total_count`.
  - **`/exports/json` returns a bare array; `/records` returns `{results:[…]}` + `total_count`.**
    Use `/records?limit=1` to read `total_count` cheaply (universe size, per-series obs count),
    and `/exports/json` to actually stream rows. Don't pull a full export just to count.
  - **Bilingual + breadcrumb for free.** The `series` row already carries EN+FR titles and a
    `path_en`/`path_fr` topic breadcrumb. Folding both languages + the path into the indexed
    `description` gives cross-language recall with **no** separate enrichment pass (contrast
    BdE, which had to spend `favoritas(idioma=en)` calls because its CSV is Spanish-only).

- **2026-06-09 — BdP refactored from scratch, now verified-live (dossier: `providers/bdp.md`).**
  A code-clean 0.7-sweep connector (keyless BPstat, archetype B) that hid a real completeness
  bug *and* a 10× efficiency win, both invisible to the existing tests. Transferable lessons:
  - **A paginated *list* endpoint is a completeness trap one level up from the data.** The bug
    wasn't in the series crawl — it was that the **datasets list** (`/domains/{id}/datasets/`)
    paginates at 10/page and the old code read only page 1. 3 of 65 domains have >10 datasets
    (one has 25), so every dataset past the 10th — and all its series — silently vanished. When
    a crawl has N levels, **every** level that paginates must be followed; audit each list call
    for an `extension.next_page` / `total > page_size`, not just the leaf.
  - **Find the per-parent declared count and use it as a free completeness oracle.** Each BPstat
    domain declares `num_series` and each dataset stub declares its own `num_series`; they sum to
    the same universe total (72,063). That lets you (a) verify the crawl yields *exactly* the
    declared count per dataset, in-band, at build time, and (b) pick the two stress cases that
    break naive crawlers — the **most-datasets** domain (catches the list-pagination bug) and the
    **deepest single dataset** (catches detail-pagination) — and prove completeness on those two
    rather than crawling all 72k. Both matched exactly.
  - **A "page size" cap and a "suppress the payload" param together turn a 7,200-page crawl into
    720.** The detail endpoint defaults to 10 series/page and caps `page_size` at 100 — but a
    bare `page_size=100` **502s** because it tries to serialise full observation history for 100
    series. Pairing it with `obs_last_n=1` (you only want the series *ids*, not their data)
    shrinks the value array to one point and the 100-series page succeeds. When a metadata crawl
    is forced through a data endpoint, hunt for the param that minimises the unwanted data
    payload — it often unlocks a larger page size too.
  - **The richest search text can live on a *different* endpoint than the enumeration.** The
    hierarchy crawl yields only a terse `label`; `/series/?series_ids=` (≤100 ids/call, **no
    observations**) returns a full breadcrumb `description` in either language. So the clean split
    is: crawl = ID discovery, `/series/` = metadata enrichment (EN primary + PT folded for BM25
    cross-language recall — the bilingual move from BdF/BdE, but sourced from a second call rather
    than the same row). Reuse the bde retry/split-on-failure batcher.
  - **`drf-yasg` + ReDoc docs ⇒ the real OpenAPI spec is one query param away.** The docs page is
    a JS SPA (`WebFetch` sees only the title), but a drf-yasg site serves the machine-readable
    spec at `…/<docs-path>/?format=openapi`. Read `redoc-init.js` for `specURL = currentPath +
    '?format=openapi'`. The 30 KB spec is the authoritative endpoint/param list — far better than
    guessing or scraping.
  - **Drop schema columns you can't fill reliably.** The old enumerator carried
    `frequency`/`units`/`start_date`/`end_date` parsed from fragile JSON-stat dimension blocks —
    but frequency is itself a *dimension* (per-series, not per-dataset), so a dataset-level value
    is wrong. The frequency/unit words already ride in the prose `description` in both languages;
    a leaner, robust schema beat a richer, sometimes-wrong one.

- **2026-06-09 — BLS refactored from scratch, now verified-live (dossier: `providers/bls.md`).**
  The guidebook's own cautionary "shallow enumerator" case. The old enumerator crawled only
  `timeseries/popular` (~top series per survey) — never a complete catalog. Transferable lessons:
  - **When the universe is uncatalogable, that *is* the finding — and the answer is structural,
    not bigger.** BLS has no flat "list all series" API; the authoritative universe is the
    per-survey `<survey>.series` flat files on `download.bls.gov`, which sum to **15.6 GB / ~tens
    of millions of series** (the injury-microdata surveys `ca`/`cb`/`cs`/`ch` alone are ~12 GB).
    You cannot embed that, and most of it is county×industry×demographic cross-products nobody
    searches for. Measure the universe *first* (sum the `.series` byte sizes from the directory
    listing) so the infeasibility is a number, not a hunch — then design for it instead of
    pretending a `popular`-list is "the catalog."
  - **The sdmx two-tier shape generalises to any huge hierarchical provider.** survey ≈ SDMX
    dataflow; a survey's dimension code tables ≈ a DSD's codelists; a BLS `series_id` ≈ a composed
    series key. So: tier-1 = an always-built, *complete* survey index carrying a compact dimension
    **manifest** (codes + labels); tier-2 = per-survey series catalogs built only for the headline
    surveys and **lazy-built + LRU-cached** on demand for the rest (the `sdmx` `CatalogLRU` +
    `resolved_catalog_url` + build-callback pattern). Completeness becomes honest and bounded: the
    *survey* catalog is total, *series* catalogs are complete per built survey, and **every** series
    is fetchable by id — the gap is discovery, not access. Separate the two questions (Q1 catalog
    vs Q2 access) and a "too big to catalog" provider still scores Q2=YES.
  - **Split the access wall from the data wall.** Two hosts, two transports: `api.bls.gov` (the
    fetch path) is plain-httpx-friendly, but the bulk flat-file host `download.bls.gov` is **Akamai
    bot-managed** and returns a 200 "Access Denied" HTML page to *every* stock UA — a browser
    `User-Agent` is not enough, only a real Chrome TLS handshake passes. Reuse the `rba` recipe:
    `curl_cffi` `impersonate="chrome"` as a **hard dep** + a hand-written status mapper (the raw §6
    carve-out). Don't let one walled host force the whole connector onto curl_cffi.
  - **The bulk `.series` file is self-describing — title + dimensions + date range in one TSV.**
    Most surveys carry a ready-made `series_title` (no dimension-join needed); a few (SM/JT/PR)
    don't, so compose a searchable title by joining the resolved dimension labels (the sdmx
    `compose_series_title` fallback). Emit `<dim>_code` + `<dim>_label` per dimension so structured
    `FIELD: value` search resolves on labels, and derive the tier-1 manifest from those same
    entries (no extra fetch). The directory listing's file *size* is a free oracle: gate the
    on-demand build on it so an agent's `*_search` never tries to index a GB-scale microdata file —
    refuse with "construct an id from the manifest and fetch it" instead.

- **2026-06-09 — BoC re-run through the full process, now verified-live (dossier: `providers/boc.md`).**
  An already-decent archetype-A connector (the roster's "gold-standard self-tracking catalog")
  that still hid a missing fetch guard and a catalog-noise wrinkle, both invisible to the
  existing tests. Transferable lessons:
  - **A multi-entity fetch endpoint can cap the *request URL*, not the entity count — guard the
    URL length, not a count.** BoC's `/observations/{names}` 302-redirects (to an error page)
    once the request URL exceeds ~4096 bytes. The boundary is on URL length: 140 *short* names
    (876 chars) pass, 140 *long* names (5,809 chars) 302 (measured full-URL 4087 ok → 4127 fail).
    A series-count cap would be both wrong (rejects valid short-name batches) and unsafe (admits
    oversized long-name batches). Guard the assembled URL pre-network with an actionable
    `InvalidParameterError` ("split, or fetch a whole panel with `group:NAME`") so the agent
    never eats an opaque 302 that `fetch_json` would surface as a `ParseError`. Pin the real
    boundary by binary-searching the URL length, then sit conservatively under it (we cap host+
    path at 4000 so the date query still fits under 4096).
  - **An enrichment fan-out doubles as a liveness probe — use its 404s to prune dead catalog
    rows.** BoC's `/lists/groups` includes ~29 retired one-off panels (dated `EXP_*`/`FSR_*`
    report bundles) that 404 on *both* `/groups/{name}` and `/observations/group/{name}`. The
    catalog already fans out `/groups/{name}` for series→group membership, so that same pass
    tells you which groups are dead — prune them so the catalog never offers an unfetchable
    panel (the "don't catalog endpoints that 404" rule, enforced cheaply). Crucial nuance:
    **prune only on a *definitive 404*, keep a *transient* 5xx/network failure best-effort** —
    else a blip silently shrinks the catalog. Log a `pruned/transient/total` summary (§7.2).
  - **Prove a flat full-index endpoint is complete by diffing a second, independent walk against
    it.** Archetype A's promise ("one call lists everything") is only a *claim* until checked.
    For BoC the independent walk is the group fan-out: unioning all 2,441 groups' memberships
    gave 15,279 distinct series and **0** that were absent from `/lists/series` (15,638) — i.e.
    the master list is a proven superset, nothing is reachable-via-group-but-unlisted. When a
    provider exposes the universe two ways (a flat index *and* a hierarchy), diff them; equality
    (or subset) is the completeness proof Q1 wants.
  - **"Listed but returns nothing recently" ≠ "un-fetchable" — re-probe over full history before
    calling an entry stale.** A 300-series fetchability sample looked like 34% empty over the
    recent 5 years, but re-probing those with *no date filter* showed all-but-one carried real
    historical data (discontinued `FSR_*` review charts, `SWP-*` working-paper series, retired
    `V*` codes). Only 1/300 was truly empty (a `200` with `observations:[]` → a clean
    `EmptyDataError`). Government statistical indexes are thick with historical series; gate
    fetchability on full-history data, not a recent window, or you'll over-report a "stale tail."
  - **`curl: (18) transfer closed` is often cosmetic — confirm with the real client before
    "fixing" it.** Raw `curl` warned about the chunked-close on BoC's 3.5 MB series index, but
    `httpx` (the kernel transport) read all 15,638 entries with no error. Don't add robustness
    code for a curl-only artifact; reproduce the failure through the actual transport first.

- **2026-06-09 — BoJ re-run through the full process, now verified-live (dossier: `providers/boj.md`).**
  A code-clean archetype-C+B connector (frozen 50-DB registry × live per-DB `getMetadata`
  fan-out, multi-bundle two-step search) that hid a **silent fetch-truncation** bug invisible to
  its tests, while its completeness story (50 DBs / **326,466 series**) had never been measured.
  Transferable lessons:
  - **A fetch endpoint can truncate at HTTP 200 "success" and hand you a resume cursor —
    paginate it, don't trust the status.** BoJ's `/getDataCode` caps each request at 250 codes
    *and* **60,000 data points** `(series × periods)`; over the point cap it returns
    `STATUS:200, MESSAGE:"Successfully completed"` with only the first *K* series and a
    top-level `NEXTPOSITION` integer. The old `boj_fetch` never read it, so any multi-series
    request silently dropped its tail (22 daily FX series → 5 returned, 17 lost, no error). This
    is the BoC URL-cap lesson in a new guise: **a 200 + "completed" is not proof of
    completeness — look for a continuation token.** Pin the truncation *granularity* live (here
    it is series-position-based, so `startPosition=NEXTPOSITION` resumes losslessly — verified
    22/22 reassembled across 3 pages) and add a non-advancement guard so a pathological cursor
    cannot loop. No single BoJ series exceeds the 60k cap (the longest, daily-from-1882, is
    52,470 points), so per-series resume is always complete — but *measure* that before relying
    on it.
  - **When the provider exposes no way to enumerate its top-level containers, freeze the
    registry but cross-validate it against a machine-readable source AND commit the harvester.**
    BoJ has no DB-list endpoint (`getMetadata` *requires* a `db`; `getDbList`/`getStatsList`
    404), so the 50-DB list is archetype C. But BoJ ships `api_tool.xlsx` with a machine-readable
    `DB_Name` sheet — so the freeze is *checkable*: a `harvest_databases.py` re-derives the 50
    `(code, category, title)` triples from the XLSX and `--diff`s them against the frozen tuple
    (zero diff, 2026-06-09). This is the SNB cautionary tale done right (SNB's `discover_cubes.py`
    never existed): a frozen registry is only safe if a committed reproduction script + a
    floor/shape test prove it still matches source.
  - **For a per-container fan-out catalog, the Q1 proof is "the per-container index endpoint is
    uncapped" — verify it returns everything in one call.** Unlike `getDataCode`, `getMetadata`
    carries **no `NEXTPOSITION`/row cap** (confirmed by diffing the top-level response keys
    across all 50 DBs): it returned all **166,513** series of the CO/TANKAN DB in a single
    ~99 MB response. That one fact is what makes each per-DB series catalog complete. Check the
    index endpoint for a pagination marker *before* trusting a single call to be exhaustive — the
    same endpoint family (`getDataCode`) *does* cap, so don't assume the metadata sibling behaves.
  - **Measure the universe up front — it can be orders of magnitude bigger than the old code
    implies, and that dictates the catalog architecture.** BoJ is **326,466 series** (CO alone
    166k; six DBs >17k), not the "~15k" the prior framing suggested. Summing per-DB series counts
    (and noticing CO's payload is 99 MB) up front is what tells you the catalog must be
    multi-bundle + lazy (the bls/sdmx two-tier) and that the enumerate fan-out should
    parse-and-release each payload rather than hold all 50 at once. Q1/Q2 then answer like bls:
    the databases tier is complete, each per-DB series tier is complete, every series is
    fetchable by id — the gap is discovery convenience, not access.

- **2026-06-09 — Destatis re-run through the full process, now verified-live (dossier: `providers/destatis.md`).**
  A code-clean 0.7-sweep connector that already had the exemplar layout but had **never been
  completeness-verified**, and whose fetch parser hard-failed a quarter of the universe on a bug
  its tests never exercised. The catalog side was actually sound — the lessons are about the
  *fetch* parser and the scope boundary. Transferable lessons:
  - **Detect a JSON-stat (or any dimensional) time axis by the SHAPE of its category keys, never
    by dimension name — and never fall back to dimension 0.** GENESIS tables carry no JSON-stat
    `role` and null labels; the time axis is the dimension whose category *index keys are the
    period values* (`JAHR`→`2012`, `STAG`→`1999-12-31`, `SEMEST`→`2003-10P6M`,
    `SMONAT`→`2015-05P1M` — ISO-8601 durations). The old parser matched dimension *names*
    (`ZEIT/JAHR/MONAT/QUARTAL`) and, on a miss, **fell back to dim 0 — the constant `statistic`
    dimension** — so every reference-date/semester/ISO-duration table emitted the *statistic code
    as a year* and raised `ParseError: year 12411 is out of range`. That was **~25% of all
    tables**, hard-failing, and invisible to a suite whose only fixture was an annual `JAHR`
    table. Two compounding traps: name-matching **misses** the real time dims, AND it
    **false-positives** lookalikes — `MONAT`/`QUARTG` here are *month-/quarter-of-year
    classifications* (keys `MONAT10`/`QUART3`), not the time axis. Key-shape detection (the
    dimension whose keys are majority period-shaped) solves both; dim 0 is never the answer.
  - **Sample fetch across the FULL spread of frequencies/structures, not one representative.**
    9/12 tables passing looked fine until the 3 failures (population/territorial/education — all
    `STAG`/`SEMEST`) exposed the whole class. One annual table would have "verified" a parser
    broken for a quarter of the catalog. The cheap audit is one table from each of a dozen
    *different* statistics, asserting real dates + zero NaT, not just "no exception".
  - **A 404 on a child-list endpoint can mean "legitimately zero", not "fetch failed" — don't let
    it drop the parent.** A tableless statistic (`61121`: `/tables`→404, `/information`→`[]`) was
    discarded by enumerate's "both sub-fetches empty → skip" guard, silently removing a real
    statistic from the catalog. Emit the parent row from the index node it already has; reserve
    the "failed" log for genuine transient double-failures (and still emit the row).
  - **Distinguish "all of the provider" from "all of the keyless API you're wrapping", and prove
    the boundary with 404s.** This keyless REST API exposes **3,009 predefined tables / 331
    statistics**; classic GENESIS *cubes* (custom-table building) are a registration-gated
    surface **absent from this host** — every `/cubes`,`/data/cube`,`/metadata/*` path 404s.
    That's a defensible scope limit (like bls microdata or boj `getDataLayer`), but only because
    it was *checked*: a fetch-beyond-catalog probe confirmed the enum gaps (`61111-0008/0009/0012`)
    404 rather than fetch, so the enumerated set == the fetchable set on this host. Don't claim
    "complete" for the provider when you mean "complete for the free API" — say which, and verify
    nothing is fetchable-but-unlisted before signing off.

- **2026-06-09 — EIA re-run through the full process, now verified-live (dossier: `providers/eia.md`).**
  The roster's last "shallow enumerator, no catalog" middle-ground case (a top-level-routes-only
  enumerator, no search connector). Turning it into a real catalog surfaced a data-loss bug *and*
  a whole out-of-tree fetch surface. Transferable lessons:
  - **Live-probing is not a substitute for reading the docs — it structurally cannot find what
    isn't in the tree you're walking.** The route-tree walk (232 leaf datasets) is the
    authoritative *catalog* enumeration, but EIA's `/v2/seriesid/{id}` legacy path — which fetches
    a series straight from its well-known v1 id (`PET.RWTC.D`, `ELEC.SALES.CO-RES.A`), the
    addressing the whole EIA/FRED ecosystem uses — is **not a tree node**, so no amount of walking
    reveals it. Only the technical docs did. The same doc-read also gave EIA's own universe figure
    ("2M+ data series") and the redistribution terms (US-federal public domain). The guidebook's
    step-2 ("compile ALL the documentation first") is not ceremony: do it *before* you design, or
    you ship a connector that silently omits a first-class access path. The honest model is
    two-surface: a route-tree **catalog** (datasets) *plus* a documented out-of-tree **id fetch**
    (`eia_fetch_series`).
  - **A paginated data endpoint with a hard row cap is the destatis/boj truncation trap in REST
    form — read the total, page the offset, and DON'T trust a sort.** EIA caps every `/data` (and
    `/seriesid`) response at 5,000 rows; the single-call predecessor returned 5,000 of
    `petroleum/pri/spt` daily's **91,285** (94.5% loss) with no error. Fix: read `response.total`
    (the count is offset-independent), page by `offset` until you have it all. Counterintuitive
    catch: the docs *recommend* `sort[0][column]=period` for pagination, but adding it
    **introduces a boundary gap+duplicate** when many rows share a period (collected == total yet
    one row missed + one repeated — the matching count *masks* the loss). Unsorted offset paging is
    empirically lossless; page unsorted and **dedup on the natural key** as insurance. Always pin
    pagination losslessness by diffing collected-unique against `total`, not by trusting either the
    status or the count alone.
  - **Cap an unbounded fetch with the provider's own narrowing guidance, not a silent truncation
    or a runaway crawl.** EIA's biggest slice (`electricity/rto/region-data` hourly) is **18.7M
    rows**; paging it whole is as wrong as truncating it. Read `total` on page 1 and, above a
    ceiling, raise `InvalidParameterError` echoing EIA's own "constrain with facet, start, end"
    message — the agent then narrows via `eia_facets` values. A facet-value discovery verb is what
    makes a huge faceted dataset usable: the catalog row lists the facet *ids*, `eia_facets` lists
    a facet's *values*, and only then can the agent build a bounded query.
  - **A faceted "dataset" is a dimension manifest, not a series — catalog the manifest, fetch the
    series.** EIA's 232 datasets expand (via the facet cartesian product) to ~2M series — the BLS/
    SDMX situation. Catalog the datasets with their measure + facet vocabulary folded into the
    indexed description (so "heat content" or "respondent" is findable), and make every series
    fetchable by route+facets or legacy id. Q1 (catalog) and Q2 (access) separate cleanly: 232/232
    datasets catalogued, 2M series all fetchable, the gap is discovery convenience not access.
  - **Prove a route-tree enumeration by diffing an independent walk (the boc move, again).** The
    enumerator and a separate throwaway tree walk agreed exactly (232 == 232, 0 either-way), and
    `response.total`-vs-collected pinned each fetch. Cross-check the measure column too: EIA names
    it inconsistently (`value`+`units` vs `sales`+`sales-units`), so detect it by the `{col}-units`
    sibling rather than assuming `value` — a hardcoded `value` silently NaNs every electricity
    series the seriesid path returns.

- **2026-06-09 — RBA re-run through the full process, now verified-live (dossier: `providers/rba.md`).**
  The archetype-E exemplar (3-pass HTML scrape; no API) and the guidebook's named
  "catalog ⊋ connector" cautionary case. The catalog side was already broad; the work was
  closing the fetch gap and hardening the XLSX layer, both proven by a live audit. Transferable
  lessons:
  - **A catalogued-but-unfetchable row is a bug to fix, not a permanent "PARTIAL by design".**
    rba enumerated XLSX-exclusive (the Bond Purchase Program) and ~480 legacy xls-hist rows, but
    `rba_fetch` resolved *only* CSV stems — so ~487 series were discoverable yet not fetchable.
    The fix was small once framed right: make the fetch verb resolve the `table_id` across every
    publication format the enumerator already reads (a `/` separates a workbook stem from a
    sheet; `hist` in the stem routes to the xls-hist host), and write **one row-matrix melter**
    that the CSV, openpyxl, and xlrd paths all share. Distinguish *inaccessible* (a real
    exclusion — premium tier, binary blob) from *merely unimplemented* (the data is right there
    in a file you already download for metadata); close the latter. The `source=` dispatch column
    the enumerator already emits is exactly the routing signal the fetch verb needs.
  - **Replace a hardcoded "exclusive" allow-list with dynamic exclusivity computed against the
    bulk pass.** rba's old XLSX pass hardcoded `{a03: Bond Purchase Program}` as the one sheet
    not republished as CSV. Instead, collect the bulk pass's id set (the 3,958 CSV series ids)
    and emit a workbook series only if its id is **not already covered** — the archetype-C/E
    hardcoded-registry liability dissolved, the pass self-maintains (a future XLSX-only sheet is
    caught automatically), and a live audit of all 5 current XLSX confirmed it yields *exactly*
    the 7 Bond Purchase Program series the allow-list named. Guard it: skip the exclusivity diff
    if the bulk pass produced nothing (a total CSV outage must not flood the catalog with
    false-exclusives).
  - **Audit the layers you *skip*, not just the ones you crawl — prove redundancy with a count.**
    RBA's tables page also links ~70 `*hist.xlsx` long-history workbooks and 11 period-range
    archives the connector ignores. Rather than assume they're redundant, I diffed a sample's
    series ids against the CSV-covered set: **0 exclusive** across all of them (same ids, longer
    history only). Now the skip is a *documented, measured* choice, not a blind omission — the
    flip side of "don't catalog endpoints that 404." A skipped layer needs the same "what would
    it add?" proof a crawled layer needs.
  - **For a no-API provider, the third-party client library *is* the spec — and a free completeness
    oracle.** RBA publishes only CSV/XLSX spreadsheets; its de-facto clients (`readrba`,
    `raustats`) are themselves scrapers, which confirms archetype E is *structural*. Reading
    `readrba`'s source gave the canonical data model (the workbook is the artifact; CSV is a
    per-sheet export; read every sheet bar `Notes`/`Series breaks`), its hardcoded
    *unreadable-table* set (the genuinely non-standard workbooks — individual-bank balance sheets,
    occasional papers — that even it excludes, matching our 0-series parse of them), and a
    universe count (~4,354) to cross-check our 4,672 catalog entries against. When there's no API
    and no spec, the most-used client library is the documentation — mine it first (guidebook
    §1.1 source #8), exactly as the EIA docs-first lesson demands.

- **2026-06-09 — SEC EDGAR re-run through the full process, now verified-live (dossier: `providers/sec_edgar.md`).**
  A native-search "commercial" provider (in `EXCLUDED_COMMERCIAL_PROVIDERS`) — and the first
  one in the sweep that is **not a timeseries source**. The package shipped 4 verbs that
  UNVERIFIED-LIVE'd (no `SEC_EDGAR_USER_AGENT` in `.env` → integration skipped); the re-run
  expanded it to 7, fixed two silent-data-loss bugs, and proved every verb live. Transferable
  lessons:
  - **"Excluded because it has native search" must mean the connector actually *wraps* that
    search — verify, don't assume.** sec_edgar was classified no-catalog because EDGAR has
    first-party search, yet its only discovery verb was a ticker-map lookup over the ~10.4k
    exchange-listed issuers — **~1.3% of the ~800k+ filers**, with no content search at all. The
    actual native surface, EDGAR full-text search (`efts.sec.gov`, filing *content* 2001→present,
    all filers), was unwrapped. The "native search ⇒ no catalog" exclusion was aspirational, not
    real. When a provider sits in the excluded-commercial list *because* it has search, open the
    connector and confirm the search verb exists and covers the universe; otherwise Q1 (discovery)
    is silently a sliver, and the exclusion is hiding a gap rather than documenting a strength.
  - **Not every provider is a timeseries — enumerate the atomic units and give each its access
    path.** EDGAR has **four** (registrant / filing / document / XBRL fact), so completeness is
    per-unit, not one fetch: discovery (full-text search + ticker map), a filer's filings, a
    document body, and XBRL three ways — per-company concept *history* (the one timeseries-shaped
    surface), all-concepts *facts*, and a cross-company *frame* (one concept, one period, every
    filer). Shoehorning a filings/documents provider into a single series-fetch leaves most of it
    unreachable. Map the model in §1.2 *before* designing verbs; the count of atomic units is the
    count of access patterns you owe.
  - **A columnar JSON feed that inlines only "recent N" with an overflow-file list is the
    destatis/boj/eia truncation trap in a new dress.** EDGAR submissions inline only the
    most-recent ≥1000 filings (`filings.recent`); the rest page into `filings.files[]` additional
    JSON files. Reading only the inline block silently drops a prolific filer's history (JPMorgan:
    1000 inline of ~158k, 67 overflow pages). The pattern to internalize: any `recent`/`latest`
    block deserves a hunt for a sibling overflow pointer, and an opt-in walk (`include_older`)
    plus a `form` filter so an agent can reach, e.g., every 10-K back to the 1990s without
    dumping 158k rows.
  - **Resolve a document by the directory's machine-readable listing, not a possibly-rendered
    pointer.** EDGAR's `primaryDocument` field can name an XSL **viewer** subpath
    (`xslF345X06/form4.xml`, not the raw `form4.xml`), and it's only present for the recent
    window — so resolving through it both mis-targets and fails for old filings. Every accession
    folder carries an `index.json` listing its real files; resolving the primary document from it
    works for **any** filing however old, returns the raw document, and removes the dependency on
    the paginated submissions feed entirely. When a provider hands you a "primary document"
    pointer, check whether it's a render path and whether a per-container directory listing is the
    more robust, universal resolver.
  - **Docs-first paid off twice, and the transport blocked itself as live proof.** `WebFetch`'s
    generic User-Agent got an HTTP 403 — a live confirmation of SEC's fair-access UA mandate
    before a line was written. Reading SEC's API page (not the existing code) is what surfaced the
    submissions pagination spec, the frames period grammar (`CY####`/`CY####Q#`/`CY####Q#I`), and
    the full-text-search `dateRange=custom` rule (omitting it with explicit `startdt`/`enddt`
    returns HTTP 500 — verified live). None were discoverable by poking the single endpoint the
    old connector already knew. Keyless-but-UA-required is also its own auth shape: model it as a
    non-secret env-resolved header with a pre-network fast-fail, never via `secrets=`/`bind` (a
    header isn't logged or redacted).

- **2026-06-09 — SNB re-run through the full process, now verified-live (dossier: `providers/snb.md`).**
  The guidebook's own named cautionary frozen-registry case (`_KNOWN_CUBES` of 237, comment citing
  a `discover_cubes.py` that never existed). Deep docs exploration didn't just fix the liability —
  it found that the connector was missing **~79% of the provider**. Transferable lessons:
  - **`robots.txt` → `/sitemap` is often the authoritative, unwalled, self-tracking enumeration —
    check it before trusting a frozen registry or a walled nav API.** SNB's docs and the old code
    pointed at a navigation-tree endpoint (`/json/structure/getNavigationTree`) that is WAF-walled
    to non-browser clients (returns an `/error_path/` HTML page), which is *why* the author froze a
    hand-harvested list. But `robots.txt` advertised `Sitemap: /sitemap` — a 1.9 MB XML listing
    **every** cube URL, open to a plain GET. Parsing it gave 237 publication-cube ids that matched
    the frozen registry **exactly (0 drift)** — the harvest was complete, it just had no committed
    reproduction. The fix turns archetype C→A: discover live from the sitemap (self-tracking) +
    commit the `harvest_cubes.py` the old comment promised. A sitemap also carries the topic/group
    path segment for free (a category signal). When the docs route you to a walled discovery API,
    look for the SEO sitemap first.
  - **A "frozen registry" smell can hide a far bigger gap than staleness — measure the whole
    universe, not just the registry's slice.** The cautionary tale was framed as "237 cubes, no
    repro script." But the same sitemap that lists the 237 publication cubes also lists **912
    data-warehouse cubes** (`/warehouse/{group}/cube/{sdmx_id}`) — the SDMX-style granular store the
    old connector *explicitly excluded* with a one-line "those aren't fetchable" comment. They are
    fetchable; the exclusion was never verified. The provider was 237 of 1,149 cubes (~21%). When
    you find a deliberate exclusion in a connector, re-test it live before trusting it (the rba/eia
    "catalogued-but-unfetchable is a bug, not a permanent exclusion" lesson, one level up: here it
    was *uncatalogued-and-untested*). Surface the scope decision to the user — they chose the full
    universe.
  - **A WAF that walls an internal API often checks one header, not a TLS fingerprint — drive the
    real SPA once and read its XHR to find the unlock.** SNB's `/json/...` metadata API
    (`getCubeInfo`, the only live source of cube *titles* — the `/api` data + dimensions payloads
    carry none) is fronted by an Airlock WAF. Stock `curl`/`httpx` got the `/error_path/` page;
    guessing headers failed. Loading one cube page in a real headless Chrome and listing the network
    requests showed every call carrying `x-epb-ajax: true` — sending just that header (no cookie, no
    `curl_cffi`) unlocked the API and turned the WAF page into real JSON with actionable 400s. The
    same capture cracked the **warehouse fetch id transform** (`getApiLinks` builds the download URL
    as `…/cube/BSTA.SNB.AUR_U.ODF/...` — the portal id's `@` becomes `.`), which no amount of
    black-box probing had revealed (`@`/`%40` → `500 illegal characters`). When an endpoint is walled
    or its id-encoding is opaque, the browser DevTools network panel *is* the spec — read the
    request the SPA actually sends. Keep such a reverse-engineered call **best-effort** (titles
    degrade to a synthesized `{cube_id} — {topic}` if it ever breaks); the completeness surface
    (sitemap + official `/api` fetch) must never depend on the internal API.
  - **Substring frequency/keyword maps have false negatives — pin them with the real strings.**
    `"day" in "daily"` is `False` (it is d-a-**i**-l-y); the freq normalizer silently returned
    "Unknown" for "Daily" until a unit test caught it. When mapping freeform provider labels by
    keyword, test each bucket against the *actual* label the provider emits, not the bucket name.

- **2026-06-09 — Treasury re-run through the full process, now verified-live (dossier: `providers/treasury.md`).**
  The guidebook's own archetype-D exemplar — and the provider the "treasury-trap" is named
  after. A 635-line monolith that had never been completeness-verified. The catalog universe
  was actually sound; the lessons are about *searchability*, the danger of grep-based schema
  claims, and what archetype-A+D discipline looks like. Transferable lessons:
  - **Name the catalog's prose column `description`, or `discovery_indexes` never indexes it —
    a silent "the catalog only searches titles" bug.** `discovery_indexes(entries)` builds
    indexes for exactly `code` (BM25), `title`, and `description` (adaptive). Treasury's
    enumerator emitted the rich Fiscal Data field text under a column named **`definition`**, so
    it populated no `description` index — the ~914-entry catalog was searchable on `title` and
    `code` **only**, and every measure's descriptive text was dead weight. Renaming the column
    `definition`→`description` (the bde/bdp/snb "fold prose into the indexed `description`"
    move) makes it searchable. Generalize: after building a catalog, *confirm which columns the
    index policy actually indexed* (`set(discovery_indexes(entries))`), don't assume your
    METADATA columns are searched. A column the policy doesn't recognise by name is invisible
    to search however rich its content.
  - **Never diagnose a feed's schema from `grep` or a single entry — read the column UNION
    across the current period. The live cross-check exists to catch *your* errors too.** I
    hypothesised the par-curve `BC_1_5MONTH` tenor was a hardcoded phantom and removed it,
    "supported" by a `grep`-based year-sweep showing 0 occurrences. Three compounding mistakes:
    (1) `grep "6WK"` false-matched **`26WK`** (the substring trap again); (2) I read a feed's
    columns from its **first `<entry>`**, but **OData omits null properties per-entry**, so a
    *sparse, recently-added* tenor is invisible in any single entry — only the column union
    across all rows (the parsed DataFrame's `columns`) is the real schema; (3) my sweep skipped
    **2025**, the exact year Treasury added the 1.5-month CMT point *and* the 6-week bill. The
    live cross-check (registry vs the 2025 column union) **refuted my "phantom"** — the registry
    was current and correct all along (registry == live 2025, exactly, all 5 feeds). The
    completeness mandate ("execution is the truth") is not only for the connector's claims; it
    catches the *reviewer's* claims. Before "fixing" a frozen registry, prove it's actually
    stale against the live current-period schema — a registry that someone kept up to date is
    the happy case, not a bug.
  - **A provider can be archetype A on one transport and D on another — make the D part safe with
    a committed harvester that diffs against the live *current-year* columns.** Treasury's Fiscal
    Data half is archetype A (one `/services/dtg/metadata/` GET returns 56 datasets → 879
    measures, self-tracking — the JSON the fiscaldata SPA itself consumes). The ODM
    interest-rate half is archetype D (a 5-feed curated registry; there is no "list all feeds"
    endpoint, but the interest-rate-statistics dropdown is the stable authoritative list, 5/5).
    The per-feed maturities are the freezable part — so `scripts/harvest_rate_feeds.py` + a live
    integration test diff the registry's benchmark columns against the live feed's column union
    (the boj/snb "freeze + committed reproduction" discipline). Both confirmed registry == live
    2025 exactly. The harvester is also how a *future* tenor addition (Treasury keeps adding
    them: 4-month 2022, 1.5-month + 6-week 2025) gets caught.
  - **Check the namesake trap, then document the real out-of-band family.** The guidebook's
    "treasury-trap" (famous series on a different subdomain / out-of-tree fetch paths) — checked
    live: there is **no** out-of-tree fetch path (every Fiscal Data fetch is by `endpoint_txt`;
    no series-id addressing, unlike EIA's `/v2/seriesid`). The ODM daily feeds are the
    famous-series-on-another-subdomain, already handled. The one genuinely separate family —
    the **HQM Corporate Bond Yield Curve + Treasury Coupon Issues** — is a binary `.xls` 5-year-
    archive product (monthly, actuarial) that is **already in FRED** (`HQMCB20YR` …), so it is a
    documented exclusion deferred to the sibling FRED connector (the alpha_vantage→FRED
    precedent), not a hidden gap. Also confirmed 3 datasets are static-file-only (no
    `endpoint_txt`) → legitimately uncataloguable. Distinguish "all of the provider" from "all
    of the JSON API" and prove the boundary (here: 18/18 sampled endpoints fetch live; the 3
    excluded carry no API path).

- **2026-06-10 — Riksbank re-run through the full process, now verified-live (dossier: `providers/riksbank.md`).**
  The guidebook's archetype-D example. The deepest Q2 gap found so far: the connector covered
  only **2 of the Riksbank's 5 public REST APIs**. The fix more than doubled the universe (124 →
  156 units, 4 → 7 connectors). Transferable lessons:
  - **The treasury-trap scales to whole APIs, not just stray series — enumerate the provider's
    PRODUCTS before trusting any connector's scope.** The old `riksbank` shipped SWEA + SWESTR
    and a docstring asserting "the forecasts API 404s on every probed path, so it is *not
    implemented*." It had probed `forecasts/v1`; the real product is `monetary_policy_data/v1/forecasts`.
    Worse, two *more* whole products (Turnover Statistics, securities Holdings) sat on the same
    gateway, unwrapped and unmentioned. The authoritative scope check is the provider's **own
    product list** — for an Azure APIM gateway, the dev-portal `/developer/apis?...` management
    endpoint (returns JSON, 200 unauthenticated; read it via DevTools on the `/apis` SPA page).
    It named all five: SWEA, SWESTR (`tora-api`), Monetary Policy Data, Turnover (`selma-api`),
    Holdings (`asset-management-api`). **Never let a predecessor connector define the universe;
    let the provider's catalog of APIs define it.**
  - **"This family isn't fetchable / 404s" comments are stale assumptions until re-probed.** Two
    "exclusion" comments were both wrong: forecasts "404s" (wrong base URL) and Holdings was
    *implicitly* parquet-only. Re-probing live: forecasts returns clean JSON at the right path,
    and the Holdings data endpoint **serves JSON by default** despite its metadata advertising
    `file_format: parquet` — so no pyarrow dependency. Probe the actual endpoint before importing
    a heavy dependency *or* before excluding a family on a predecessor's word.
  - **A literal `:` in a query value: httpx `%3A`-encodes it, and a strict gateway 404s — which a
    naive client turns into a silent whole-universe fallback.** Monetary Policy filters on
    `policy_round_name=2026:1`. Passed via httpx `params`, the colon becomes `%3A` → HTTP 404;
    the shared `HttpClient` also *strips* a query embedded in the path. Both failure modes return
    "no match," and a tolerant client that retries-without-the-filter would silently fetch **all
    24 series × 59 vintages** instead of the one asked for (caught live: the response's first
    `external_id` wasn't the requested series). Fix: a small raw helper that builds the URL with
    `urlencode(..., safe=":")` and a literal colon, mapping errors like `fetch_json` (the swemo
    client's documented convention). General rule: when a value contains a sub-delimiter the
    server treats syntactically (`:`/`,`/`;`), verify on the wire whether it must stay literal,
    and don't trust 200-vs-404 alone — **assert the response is actually filtered.**
  - **One catalog over several products needs codes that self-route, because search returns only
    `code`/`title`/`score`.** `make_local_search_connector` hard-fixes the search output to those
    three columns — the `source` METADATA column does *not* reach the agent. So when a catalog
    feeds N different fetch verbs, the **code shape is the routing channel.** riksbank keeps
    SWEA/SWESTR ids bare (disjoint, self-identifying) but prefixes the three new families
    (`monetary_policy/<id>`, `turnover/<market>/<freq>`, `holdings/<dataset>`) — bare MP ids
    share SWEA's `SED*`/`SEM*`/`SEA*` prefix space, so they'd be ambiguous. Decide the code scheme
    from the number of distinct fetch verbs the catalog routes to, not just id-uniqueness.
  - **Forecast/vintage data is a real shape: a series × a vintage (policy round).** Monetary
    Policy fetch takes `series` + optional `policy_round`; with a round it returns one vintage
    (each observation list = realised history to the cutoff + the forecast horizon), without a
    round it returns *every* vintage (a `policy_round` column disambiguates). 24 series × ~59
    rounds. Catalog the series (the fetchable id); the round is a fetch parameter, like a
    dimension — the same call shape as a normal series fetch with one extra axis.
  - **When the gap is this big, ask before committing scope.** Three missing products differ in
    value and effort (Monetary Policy: flagship, clean JSON; Turnover: small faceted JSON;
    Holdings: niche, *thought* to need parquet). Rather than silently doing all or the minimum, I
    sized each live (counts + shapes) and put the scope fork to the user (the SNB-warehouse
    precedent) — "full universe" was chosen. Sizing first made the choice informed; the parquet
    fear turned out moot once probed.
