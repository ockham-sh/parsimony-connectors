"""Banque de France (BdF) Webstat transport: constants, auth, HTTP client.

BdF publishes through an Opendatasoft Explore v2.1 API. A single Opendatasoft
"catalog" hosts several system datasets; three of them carry everything this
connector needs (all live-verified 2026-06-08):

* ``webstat-datasets`` — 45 records, one per BdF dataflow (``EXR``, ``BSI``, …),
  each with bilingual ``name``/``description`` and a ``series_count``.
* ``series`` — the full series catalogue as one flat table (~41.6k records),
  each row carrying ``series_key``, ``dataset_id``, bilingual titles, the
  ``path_en``/``path_fr`` breadcrumb, frequency, reference area and source
  agency. Because it is a single queryable table, the *entire* universe streams
  from one ``/exports/json`` call — no per-dataset fan-out needed.
* ``observations`` — observation rows, filtered by ``series_key`` for a fetch.

Auth is an Opendatasoft API key sent in the ``Authorization: Apikey <KEY>``
header (the literal word ``Apikey`` — *not* ``Bearer``; the wrong scheme returns
a silent 401). The key rides the header, never a query param, so it never lands
in a request log. Register at https://developer.webstat.banque-france.fr/ and
export it as ``BDF_API_KEY``. Quota: 10,000 requests/day.
"""

from __future__ import annotations

import os

import httpx
from parsimony.errors import UnauthorizedError
from parsimony.transport import HttpClient
from parsimony.transport.helpers import make_http_client
from parsimony_shared.cb_enumerate import MetadataCrawlConfig

PROVIDER = "bdf"
ENV_VAR = "BDF_API_KEY"
USER_AGENT = "parsimony-bdf/0.8"

BASE_URL = "https://webstat.banque-france.fr/api/explore/v2.1/catalog/datasets"

# Opendatasoft export endpoints (relative to BASE_URL). ``/exports/json`` streams
# the whole (optionally filtered) dataset in one response — no pagination.
DATASETS_PATH = "webstat-datasets/exports/json"
SERIES_PATH = "series/exports/json"
OBSERVATIONS_PATH = "observations/exports/json"

# Lean column projections keep the streamed payloads small. The ``series`` table
# is ~200 columns wide (a sparse SDMX flat table) — selecting only what the
# catalog needs turns a multi-hundred-MB export into a few MB.
DATASETS_SELECT = "dataset_id,name_en,name_fr,description_en,description_fr,series_count"
SERIES_SELECT = (
    "series_key,dataset_id,title_en,title_fr,title_long_en,title_long_fr,"
    "freq,ref_area,source_agency,first_time_period_date,last_time_period_date,"
    "path_en,path_fr"
)
OBSERVATIONS_SELECT = "series_key,title_en,title_fr,time_period_start,obs_value"

# The enumerator issues only two requests (datasets + the full series export),
# so concurrency is almost moot; a small cap with a courtesy delay is plenty.
METADATA_CRAWL = MetadataCrawlConfig(inter_request_delay_s=0.25)

# The full ``series`` export is large; allow a long read. Used by the enumerator.
CRAWL_TIMEOUT = httpx.Timeout(connect=30.0, read=180.0, write=30.0, pool=30.0)


def resolve_key(api_key: str) -> str:
    """Resolve the API key (arg → ``BDF_API_KEY`` env fallback); fast-fail if absent.

    A missing key raises :class:`UnauthorizedError` naming the env var so an agent
    is told exactly which variable to set — *before* any network call is made.
    ``env_var`` is keyword-only.
    """
    key = (api_key or os.environ.get(ENV_VAR, "")).strip()
    if not key:
        raise UnauthorizedError(PROVIDER, env_var=ENV_VAR)
    return key


def auth_headers(key: str) -> dict[str, str]:
    """Build the Webstat auth + transport headers for an already-resolved key.

    Note the literal ``Apikey`` token (not ``Bearer``) — Opendatasoft's auth
    scheme is non-standard, and the wrong word yields a silent 401.
    """
    return {
        "Authorization": f"Apikey {key}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }


def make_fetch_client(api_key: str) -> HttpClient:
    """Resolve the key (fast-fail) and build the canonical client for ``bdf_fetch``."""
    key = resolve_key(api_key)
    return make_http_client(BASE_URL, headers=auth_headers(key), timeout=60.0)


__all__ = [
    "BASE_URL",
    "CRAWL_TIMEOUT",
    "DATASETS_PATH",
    "DATASETS_SELECT",
    "ENV_VAR",
    "METADATA_CRAWL",
    "OBSERVATIONS_PATH",
    "OBSERVATIONS_SELECT",
    "PROVIDER",
    "SERIES_PATH",
    "SERIES_SELECT",
    "USER_AGENT",
    "auth_headers",
    "make_fetch_client",
    "resolve_key",
]
