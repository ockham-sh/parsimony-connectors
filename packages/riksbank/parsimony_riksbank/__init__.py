"""Sveriges Riksbank (Sweden): fetch + catalog enumeration.

API docs: https://developer.api.riksbank.se/

Riksbank publishes its statistical catalog over a small family of REST
APIs. Two are surfaced here:

1. **SWEA** (interest rates and exchange rates) at
   ``api.riksbank.se/swea/v1``. The core family; ~117 series enumerated
   from ``/Groups`` + ``/Series``.
2. **SWESTR** (Swedish Krona Short-Term Rate) at
   ``api.riksbank.se/swestr/v1``. The overnight reference rate, its
   compounded averages (1W/1M/2M/3M/6M), and the SWESTR index. Not in
   SWEA — a separate API with its own URL scheme (``/latest/...``,
   ``/avg/latest/...``, ``/index/latest/...``). Catalog rows carry
   ``source="swestr"`` so an agent routes the hit to
   :func:`riksbank_swestr_fetch`.

Both SWEA and SWESTR are **open / keyless** for fetch and enumeration —
the ``Ocp-Apim-Subscription-Key`` header is optional and only raises the
quota. The keyless quota is tight (HTTP 429 after a small burst), so a
key is recommended for catalog builds.

Transport: every verb uses the canonical core helper pair
``make_http_client`` + ``fetch_json`` (GET + ``raise_for_status`` +
``map_http_error`` / ``map_timeout_error`` + ``json()`` + ``None``-param
dropping). All three Riksbank endpoints return JSON, so ``fetch_json``
fits and timeouts map to ``ProviderError(408)`` for free.

A third family, **forecasts/outcomes** at ``api.riksbank.se/forecasts/v1``,
returns ``404 Resource not found`` on every probed path as of the
catalog freeze; cataloguing 404-ing endpoints would mislead dispatching
agents, so forecasts is *not implemented*. A future enumerator extension
emits ``source="forecasts"`` rows once the endpoint returns.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Annotated, Any, Literal

import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
)
from parsimony.transport import HttpClient
from parsimony.transport.helpers import fetch_json, make_http_client

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.riksbank.se/swea/v1"
_SWESTR_BASE_URL = "https://api.riksbank.se/swestr/v1"
_ENV_VAR = "RIKSBANK_API_KEY"


#: Source identifiers for catalog rows. Exposed as constants rather than
#: scattered string literals so the dispatch contract stays greppable
#: when forecasts (or another family) is added later.
_SWEA_SOURCE = "swea"
_SWESTR_SOURCE = "swestr"

# SWESTR series identifiers exposed as a closed enum on the connector
# signature. Riksbank publishes the overnight fixing (``SWESTR``), five
# compounded averages (``SWESTRAVG1W`` … ``SWESTRAVG6M``) and one index
# (``SWESTRINDEX``). Typing the parameter as a Literal lets the framework
# reject bad values before any request rather than letting them 404.
SwestrSeries = Literal[
    "SWESTR",
    "SWESTRAVG1W",
    "SWESTRAVG1M",
    "SWESTRAVG2M",
    "SWESTRAVG3M",
    "SWESTRAVG6M",
    "SWESTRINDEX",
]

_SWESTR_IDS: frozenset[str] = frozenset(
    {
        "SWESTR",
        "SWESTRAVG1W",
        "SWESTRAVG1M",
        "SWESTRAVG2M",
        "SWESTRAVG3M",
        "SWESTRAVG6M",
        "SWESTRINDEX",
    }
)


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

RIKSBANK_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` is upstream long-form text surfaced as metadata
        # for catalog indexing and search.
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` tells the agent which fetch connector to call:
        # ``"swea"`` → riksbank_fetch, ``"swestr"`` → riksbank_swestr_fetch.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="frequency_source", role=ColumnRole.METADATA),
        Column(name="group", role=ColumnRole.METADATA),
        Column(name="provider", role=ColumnRole.METADATA),
        Column(name="observation_min", role=ColumnRole.METADATA),
        Column(name="observation_max", role=ColumnRole.METADATA),
        Column(name="series_closed", role=ColumnRole.METADATA),
    ]
)

RIKSBANK_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

# SWESTR payloads carry more metadata than a plain rate: a publication
# time, transaction volume / count / agent count (for the raw fixing),
# and 12.5/87.5 percentiles of the underlying trades. Only ``date`` and
# ``value`` are declared as named DATA columns here — the rest fold in as
# additional DATA columns so agents with SWESTR-specific analyses can read
# them without a fetch-level schema migration.
RIKSBANK_SWESTR_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

_ENUMERATE_COLUMNS: tuple[str, ...] = (
    "series_id",
    "title",
    "description",
    "source",
    "frequency",
    "frequency_source",
    "group",
    "provider",
    "observation_min",
    "observation_max",
    "series_closed",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str = "") -> HttpClient:
    """Build a SWEA client. The optional key rides in a header (never a
    query param), so it stays out of request logs without needing the
    transport sensitive-param set."""
    key = api_key or os.environ.get(_ENV_VAR, "")
    headers: dict[str, str] = {}
    if key:
        headers["Ocp-Apim-Subscription-Key"] = key
    return make_http_client(_BASE_URL, headers=headers, timeout=30.0)


def _make_swestr_http(api_key: str = "") -> HttpClient:
    key = api_key or os.environ.get(_ENV_VAR, "")
    headers: dict[str, str] = {}
    if key:
        headers["Ocp-Apim-Subscription-Key"] = key
    return make_http_client(_SWESTR_BASE_URL, headers=headers, timeout=30.0)


def _validate_date_pair(from_date: str | None, to_date: str | None) -> None:
    """Require both ``from_date`` and ``to_date`` together, or neither.

    ``from_date`` alone is ambiguous against the window-vs-latest dispatch
    (SWEA's ``/Observations/{id}/{from}/{to}`` needs both bounds; SWESTR's
    ``/all`` treats a lone ``fromDate`` as "everything since"). Exactly one
    of the pair → :class:`InvalidParameterError`.
    """
    if (from_date is None) != (to_date is None):
        raise InvalidParameterError("riksbank", "Provide both from_date and to_date, or neither")


def _swestr_kind(series: str) -> str:
    """Return the URL family for a SWESTR series id.

    ``SWESTR`` → raw fixing (``/latest`` or ``/all``);
    ``SWESTRAVG*`` → compounded average (``/avg/...``);
    ``SWESTRINDEX`` → index (``/index/...``).
    """
    if series == "SWESTR":
        return "rate"
    if series == "SWESTRINDEX":
        return "index"
    return "avg"


def _to_value(raw_value: Any) -> float | None:
    """Coerce a raw rate/index field to float, or ``None`` if absent/blank."""
    if raw_value in (None, "", "NaN"):
        return None
    try:
        return float(raw_value)
    except (ValueError, TypeError):
        return None


def _swestr_parse_rows(series: str, payload: Any) -> list[dict[str, Any]]:
    """Flatten a SWESTR response into ``{series, date, value, ...}`` rows.

    The ``/latest`` endpoints return a single JSON object; the windowed
    endpoints return a JSON list. Raw SWESTR and the averages carry a
    ``rate`` field; the index uses ``value`` instead. We normalise both
    onto a single ``value`` column so downstream code doesn't branch on
    series kind, while preserving the native metadata on each row.
    """
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        date_val = item.get("date") or item.get("Date")
        if date_val is None:
            continue
        # Index payloads use ``value`` (an index level); rate payloads use
        # ``rate``. Defensive lookups tolerate either being missing.
        raw_value = item.get("rate")
        if raw_value is None:
            raw_value = item.get("value")
        row: dict[str, Any] = {
            "series": series,
            "date": date_val,
            "value": _to_value(raw_value),
        }
        # Pass through Riksbank's confidence/volume metadata — useful for
        # analysts spotting alternative-calculation days. The ``avg``
        # endpoint carries ``startDate`` (the window start); the raw fixing
        # carries ``volume`` / ``numberOfTransactions`` etc.
        for extra in (
            "startDate",
            "publicationTime",
            "republication",
            "alternativeCalculation",
            "alternativeCalculationReason",
            "pctl12_5",
            "pctl87_5",
            "volume",
            "numberOfTransactions",
            "numberOfAgents",
        ):
            if extra in item:
                row[extra] = item[extra]
        rows.append(row)
    return rows


# Suffix → frequency mapping. Riksbank's series-id convention is
# undocumented but the suffix letter is stable across the catalog:
# ``PMI``/``PMD`` for daily fix, ``PMW``/``PMM`` for weekly/monthly,
# ``PMQ`` quarterly, ``PMA`` annual. We record both the inferred value and
# how we got it (``frequency_source``) so downstream consumers can decide
# whether to trust it.
_FREQ_BY_SUFFIX: tuple[tuple[str, str], ...] = (
    ("PMI", "Daily"),
    ("PMD", "Daily"),
    ("PMW", "Weekly"),
    ("PMM", "Monthly"),
    ("PMQ", "Quarterly"),
    ("PMA", "Annual"),
)

# Group-id → frequency mapping for buckets where the SWEA hierarchy pins
# down the publication cadence (probed via /swea/v1/Groups). Interest-rate
# and FX series publish daily at 16:15 CET; monthly/annual aggregate
# buckets carry their own group ids (133, 134).
_FREQ_BY_GROUP_ID: dict[int, str] = {
    2: "Daily",  # Riksbank key interest rates
    3: "Daily",  # Other Riksbank interest rates
    5: "Daily",  # STIBOR
    6: "Daily",  # Swedish Treasury Bills (SE TB)
    7: "Daily",  # Swedish Government Bonds (SE GVB)
    8: "Daily",  # Swedish Fixing Rates (SE STFIX)
    9: "Daily",  # Swedish Mortgage Bonds (SE MB)
    10: "Daily",  # Swedish Commercial Paper (SE CP)
    97: "Daily",  # Euro Market Rates, 3 months
    98: "Daily",  # Euro Market Rates, 6 months
    99: "Daily",  # International Government Bonds, 5 years
    100: "Daily",  # International Government Bonds, 10 years
    12: "Daily",  # Swedish TCW index
    130: "Daily",  # Currencies against Swedish kronor
    131: "Daily",  # Cross rates
    138: "Daily",  # Special Drawing Rights (SDR)
    151: "Daily",  # Swedish KIX index
    155: "Daily",  # Forward Premiums
    133: "Monthly",  # Monthly aggregate
    134: "Annual",  # Annual aggregate
}


def _infer_frequency(series_id: str, group_id: int | None) -> tuple[str, str]:
    """Best-effort frequency for a SWEA series.

    Returns ``(frequency, source)`` where ``source`` is one of ``"group"``,
    ``"suffix"``, or ``"unknown"`` so downstream consumers can tell a
    confident value from a heuristic.
    """
    if group_id is not None and group_id in _FREQ_BY_GROUP_ID:
        return _FREQ_BY_GROUP_ID[group_id], "group"
    sid = series_id.upper()
    for suffix, freq in _FREQ_BY_SUFFIX:
        if sid.endswith(suffix):
            return freq, "suffix"
    return "Unknown", "unknown"


def _flatten_groups(root: Any) -> dict[str, str]:
    """Walk the SWEA ``/Groups`` tree into ``{group_id: full_path_name}``.

    The endpoint returns a single root node with ``childGroups``; each node
    carries ``groupId``, ``name``, ``description``. We also accept legacy
    keys defensively in case the API surface ever changes.
    """
    lookup: dict[str, str] = {}

    def _walk(node: Any, parent: str = "") -> None:
        if isinstance(node, list):
            for item in node:
                _walk(item, parent)
            return
        if not isinstance(node, dict):
            return
        gid = node.get("groupId", node.get("id", ""))
        name = node.get("name", node.get("groupName", ""))
        full = f"{parent} > {name}" if parent and name else (name or parent)
        if gid != "":
            lookup[str(gid)] = full
        children = node.get("childGroups") or node.get("groupInfos") or node.get("children") or []
        _walk(children, full)

    _walk(root)
    return lookup


def _series_description(series: dict[str, Any]) -> str:
    """Pick the richest description SWEA offers for a series.

    Order of preference: ``longDescription`` (full sentences),
    ``midDescription`` (one-paragraph), ``shortDescription`` (label). SWEA
    always populates at least one of the three, but we synthesise a
    fall-back from id + provider so the DESCRIPTION column is never empty.
    """
    for key in ("longDescription", "midDescription", "shortDescription"):
        v = series.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    sid = series.get("seriesId") or "series"
    provider = series.get("source") or "Sveriges Riksbank"
    return f"{sid} — published by {provider}."


def _series_title(series: dict[str, Any], sid: str) -> str:
    """Resolve a human-readable title from a SWEA ``/Series`` entry.

    SWEA's ``/Series`` rows carry ``shortDescription`` (the UI label) and
    the longer description fields — there is no ``seriesName``/``name`` key
    despite older code looking for them. Prefer the short label, then the
    mid description, finally the id itself.
    """
    title = series.get("shortDescription") or series.get("midDescription") or sid
    return str(title).strip() or sid


# ---------------------------------------------------------------------------
# SWESTR static registry
#
# SWESTR is not in SWEA's ``/Series`` — it lives on a separate API with its
# own URL scheme (``/latest/<id>``, ``/avg/latest/<id>``,
# ``/index/latest/<id>``). The set of series is small (seven) and stable:
# the fixing, five compounded averages, and one index. A static registry is
# the right surface here — it spares the enumerator a second live API
# dependency for data that doesn't change.
# ---------------------------------------------------------------------------

_SWESTR_PROVIDER = "Sveriges Riksbank"
_SWESTR_GROUP = "SWESTR > Swedish Krona Short-Term Rate"
_SWESTR_INCEPTION_DATE = "2021-09-01"  # Official launch of published values.

#: SWESTR series definitions. ``kind`` mirrors :func:`_swestr_kind`.
_SWESTR_SERIES: tuple[dict[str, str], ...] = (
    {
        "series_id": "SWESTR",
        "kind": "rate",
        "frequency": "Daily",
        "title": "SWESTR — Swedish Krona Short-Term Rate",
        "description": (
            "SWESTR is a transaction-based reference rate calculated by the "
            "Riksbank from overnight unsecured money-market transactions in "
            "Swedish kronor. Published each banking day at 09:00 CET for the "
            "previous day. The official short-term interest-rate benchmark "
            "for Swedish kronor, designed to replace STIBOR T/N."
        ),
    },
    {
        "series_id": "SWESTRAVG1W",
        "kind": "avg",
        "frequency": "Daily",
        "title": "SWESTR — 1-week compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 1-week "
            "observation period. Published by the Riksbank at 09:05 CET each "
            "banking day, cumulative so the compounding effect is included."
        ),
    },
    {
        "series_id": "SWESTRAVG1M",
        "kind": "avg",
        "frequency": "Daily",
        "title": "SWESTR — 1-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 1-month "
            "observation period, published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRAVG2M",
        "kind": "avg",
        "frequency": "Daily",
        "title": "SWESTR — 2-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 2-month "
            "observation period, published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRAVG3M",
        "kind": "avg",
        "frequency": "Daily",
        "title": "SWESTR — 3-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 3-month "
            "observation period, published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRAVG6M",
        "kind": "avg",
        "frequency": "Daily",
        "title": "SWESTR — 6-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 6-month "
            "observation period, published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRINDEX",
        "kind": "index",
        "frequency": "Daily",
        "title": "SWESTR Index",
        "description": (
            "SWESTR compounded index. The Riksbank publishes the index daily "
            "alongside the fixing; ratios between two index values yield the "
            "realised compounded rate between those dates, enabling clean "
            "settlement math for SWESTR-referencing instruments."
        ),
    },
)

#: ``series_id`` → human title, for fetch-time title resolution.
_SWESTR_TITLE_BY_ID: dict[str, str] = {spec["series_id"]: spec["title"] for spec in _SWESTR_SERIES}


def _build_swestr_rows() -> list[dict[str, Any]]:
    """One catalog row per :data:`_SWESTR_SERIES` entry.

    Pure function — the registry is static so this involves no I/O. Each row
    carries ``source="swestr"`` so a dispatching agent routes the hit to
    :func:`riksbank_swestr_fetch` rather than :func:`riksbank_fetch`.
    """
    rows: list[dict[str, Any]] = []
    for spec in _SWESTR_SERIES:
        rows.append(
            {
                "series_id": spec["series_id"],
                "title": spec["title"],
                "description": spec["description"],
                "source": _SWESTR_SOURCE,
                "frequency": spec["frequency"],
                # The cadence is documented by Riksbank on the SWESTR
                # methodology page, not inferred from id suffix / group id.
                "frequency_source": "registry",
                "group": _SWESTR_GROUP,
                "provider": _SWESTR_PROVIDER,
                "observation_min": _SWESTR_INCEPTION_DATE,
                "observation_max": "",  # Live daily — no fixed end.
                "series_closed": False,
            }
        )
    return rows


def _normalize_observation_date(value: Any) -> str:
    """Coerce an upstream date-like field to ``YYYY-MM-DD`` or ``""``."""
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return ""
    # SWEA returns ``YYYY-MM-DD`` strings; pass through unchanged. Anything
    # weirder we leave to the embedder/BM25 as a literal.
    return s[:10]


def _resolve_swea_title(http: HttpClient, series_id: str) -> str:
    """Best-effort title for a SWEA series from ``/Series`` (``shortDescription``).

    The ``/Observations`` payload carries only ``date``/``value`` — no
    title — so the label is resolved with a secondary ``/Series`` request.
    This enrichment is non-essential: a transient operational failure of
    the secondary request (rate-limit, timeout, 5xx) must NOT fail the whole
    fetch, so we catch the typed :class:`ConnectorError` family and fall
    back to the series id. Programmer errors (``TypeError`` etc.) still
    propagate — we do not blanket-swallow.
    """
    try:
        series_list = fetch_json(http, path="Series", provider="riksbank", op_name="Series")
    except ConnectorError as exc:
        logger.warning("riksbank: title lookup for %s failed (%s); using id", series_id, type(exc).__name__)
        return series_id
    if isinstance(series_list, dict):
        series_list = [series_list]
    if not isinstance(series_list, list):
        return series_id
    for s in series_list:
        if isinstance(s, dict) and s.get("seriesId") == series_id:
            return _series_title(s, series_id)
    return series_id


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=RIKSBANK_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_fetch(
    series_id: Annotated[str, Namespace("riksbank")],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a single Riksbank SWEA time series by series_id.

    Returns date + value with the upstream series name as title. Use
    ``from_date``/``to_date`` together to fetch a window; omit both to
    receive the latest observation. SWEA is keyless — ``api_key`` is
    optional and only raises the quota.
    """
    series_id = series_id.strip()
    if not series_id:
        raise InvalidParameterError("riksbank", "series_id must be non-empty")
    _validate_date_pair(from_date, to_date)

    http = _make_http(api_key)

    if from_date and to_date:
        path = f"Observations/{series_id}/{from_date}/{to_date}"
    else:
        path = f"Observations/Latest/{series_id}"

    data = fetch_json(http, path=path, provider="riksbank", op_name="Observations")

    title = _resolve_swea_title(http, series_id)

    items = data if isinstance(data, list) else [data]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        date_val = item.get("date") or item.get("Date")
        if date_val is None:
            continue
        raw_value = item.get("value")
        if raw_value is None:
            raw_value = item.get("Value")
        rows.append(
            {
                "series_id": series_id,
                "title": title,
                "date": date_val,
                "value": _to_value(raw_value),
            }
        )

    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No observations returned for: {series_id}",
            query_params={"series_id": series_id, "from_date": from_date, "to_date": to_date},
        )

    return pd.DataFrame(rows)


@connector(output=RIKSBANK_SWESTR_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_swestr_fetch(
    series: Annotated[SwestrSeries, Namespace("riksbank")],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a SWESTR fixing / compounded average / index series.

    Dispatches across three URL families:

    * ``SWESTR``       → ``/latest/SWESTR`` or ``/all/SWESTR``
    * ``SWESTRAVG*``   → ``/avg/latest/<id>`` or ``/avg/<id>``
    * ``SWESTRINDEX``  → ``/index/latest/<id>`` or ``/index/<id>``

    ``from_date``/``to_date`` together request a window; omit both for the
    latest published value (one-row result). Returns date + value plus
    SWESTR's native metadata (publication time, percentiles, transaction
    volumes) on additional columns. SWESTR is keyless — ``api_key`` is
    optional and only raises the quota.
    """
    if series not in _SWESTR_IDS:
        raise InvalidParameterError(
            "riksbank",
            f"Unknown SWESTR series {series!r}; expected one of {sorted(_SWESTR_IDS)}",
        )
    _validate_date_pair(from_date, to_date)

    http = _make_swestr_http(api_key)
    kind = _swestr_kind(series)

    if from_date and to_date:
        # Windowed endpoints use ``fromDate``/``toDate`` query params.
        query: dict[str, Any] | None = {"fromDate": from_date, "toDate": to_date}
        if kind == "rate":
            path = f"all/{series}"
        elif kind == "avg":
            path = f"avg/{series}"
        else:  # index
            path = f"index/{series}"
    else:
        query = None
        if kind == "rate":
            path = f"latest/{series}"
        elif kind == "avg":
            path = f"avg/latest/{series}"
        else:  # index
            path = f"index/latest/{series}"

    payload = fetch_json(http, path=path, params=query, provider="riksbank", op_name=path)

    rows = _swestr_parse_rows(series, payload)
    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No SWESTR observations returned for series={series!r}",
            query_params={"series": series, "from_date": from_date, "to_date": to_date},
        )

    df = pd.DataFrame(rows)
    df["title"] = _SWESTR_TITLE_BY_ID.get(series, series)
    return df


@enumerator(output=RIKSBANK_ENUMERATE_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def enumerate_riksbank(api_key: str = "") -> pd.DataFrame:
    """Enumerate Riksbank SWEA series plus static SWESTR registry rows.

    Two cheap requests — ``/Groups`` (group hierarchy) and ``/Series``
    (~117 series in one shot, no per-series fan-out) — then appends the
    seven static SWESTR rows. SWEA is keyless; the optional ``api_key``
    raises the quota.
    """
    http = _make_http(api_key)

    # ``/Groups`` returns a single root node (not a list); ``_flatten_groups``
    # tolerates both and walks ``childGroups``.
    groups_data = fetch_json(http, path="Groups", provider="riksbank", op_name="Groups")
    group_lookup = _flatten_groups(groups_data)

    series_data = fetch_json(http, path="Series", provider="riksbank", op_name="Series")
    if isinstance(series_data, dict):
        series_data = [series_data]
    if not isinstance(series_data, list):
        raise ParseError("riksbank", "unexpected /Series response shape (expected a list)")

    rows: list[dict[str, Any]] = []
    for s in series_data:
        if not isinstance(s, dict):
            continue
        sid = s.get("seriesId", "")
        if not sid:
            continue
        group_id_raw = s.get("groupId", "")
        group_id_int: int | None
        try:
            group_id_int = int(group_id_raw) if group_id_raw != "" else None
        except (TypeError, ValueError):
            group_id_int = None
        frequency, frequency_source = _infer_frequency(sid, group_id_int)

        rows.append(
            {
                "series_id": sid,
                "title": _series_title(s, sid),
                "description": _series_description(s),
                "source": _SWEA_SOURCE,
                "frequency": frequency,
                "frequency_source": frequency_source,
                "group": group_lookup.get(str(group_id_raw), ""),
                "provider": s.get("source", "") or "",
                "observation_min": _normalize_observation_date(s.get("observationMinDate")),
                "observation_max": _normalize_observation_date(s.get("observationMaxDate")),
                "series_closed": bool(s.get("seriesClosed", False)),
            }
        )

    # Append SWESTR rows from the static registry (separate URL family, not
    # SWEA /Series — a registry, not a second live call, is the right surface).
    rows.extend(_build_swestr_rows())

    columns = list(_ENUMERATE_COLUMNS)
    df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
    return df


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_riksbank.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    RIKSBANK_SEARCH_OUTPUT,
    RiksbankSearchParams,
    riksbank_search,
)

CONNECTORS = Connectors([riksbank_fetch, riksbank_swestr_fetch, enumerate_riksbank, riksbank_search])


def load(*, catalog_url: str | None = None, api_key: str = "") -> Connectors:
    """Return :data:`CONNECTORS` with optional catalog URL and API key bound."""
    bound = CONNECTORS
    if api_key:
        bound = bound.bind(api_key=api_key)
    if catalog_url is None:
        return bound
    return bound.bind(catalog_url=catalog_url)
