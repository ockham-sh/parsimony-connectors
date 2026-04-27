"""Sveriges Riksbank (Sweden): fetch + catalog enumeration.

API docs: https://developer.api.riksbank.se/
API key optional but recommended.

Riksbank publishes its statistical catalog over a small family of REST
APIs. Two are surfaced here:

1. **SWEA** (Swedish External Asset — interest rates and exchange
   rates) at ``api.riksbank.se/swea/v1``. The core family; ~117 series
   enumerated from ``/Groups`` + ``/Series``.
2. **SWESTR** (Swedish Krona Short-Term Rate) at
   ``api.riksbank.se/swestr/v1``. The overnight reference rate, its
   compounded averages (1W/1M/2M/3M/6M), and the SWESTR index. Not in
   SWEA — a separate API with its own URL scheme (``/latest/...``,
   ``/avg/latest/...``, ``/index/latest/...``). Mirrors the Treasury
   package's dual-source design: catalog rows carry ``source="swestr"``
   so an agent routes the hit to :func:`riksbank_swestr_fetch`.

A third family, **forecasts/outcomes** at ``api.riksbank.se/forecasts/v1``,
is documented by Riksbank but every probed path (``/forecasts``,
``/indicators``, case-variants) returns ``404 Resource not found`` as
of the catalog freeze date. Third-party clients that target the same
URL scheme carry in-code comments warning it "may be temporarily
unavailable". Cataloguing 404-ing endpoints would mislead dispatching
agents, so forecasts is *not implemented* — a future enumerator
extension emits ``source="forecasts"`` rows once the endpoint returns.

A fourth family was investigated under the working name **CBA**
(Central Bank Asset) after a reference in internal Riksbank docs, but
no ``cba`` endpoint exists on ``api.riksbank.se``: every probed path
(``/cba``, ``/cba/v1/...``, ``/cba/Series``, etc.) returns
``404 Resource not found`` and the developer portal SPA exposes no
machine-readable product index. CBA is therefore *not implemented*.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Annotated, Any, Literal

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport import HttpClient, map_http_error
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.riksbank.se/swea/v1"
_SWESTR_BASE_URL = "https://api.riksbank.se/swestr/v1"

#: Source identifiers for catalog rows. Exposed as constants rather than
#: scattered string literals so the dispatch contract stays greppable
#: when forecasts/CBA (or another family) is added later.
_SWEA_SOURCE = "swea"
_SWESTR_SOURCE = "swestr"


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class RiksbankFetchParams(BaseModel):
    """Parameters for fetching a Riksbank time series."""

    series_id: Annotated[str, "ns:riksbank"] = Field(..., description="Riksbank series ID (e.g. SEKEURPMI)")
    from_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_id must be non-empty")
        return v

    @field_validator("to_date")
    @classmethod
    def _both_dates_or_neither(cls, v: str | None, info: Any) -> str | None:
        from_date = info.data.get("from_date")
        if (from_date is None) != (v is None):
            raise ValueError("Provide both from_date and to_date, or neither")
        return v


class RiksbankEnumerateParams(BaseModel):
    """No parameters needed — enumerates all Riksbank series."""

    pass


# SWESTR series identifiers exposed as a closed enum. Riksbank publishes
# the overnight fixing (``SWESTR``), five compounded averages
# (``SWESTRAVG1W`` … ``SWESTRAVG6M``) and one index (``SWESTRINDEX``).
# Making this a Literal lets pydantic catch bad values at param-validation
# time rather than as a 404 from Riksbank.
SwestrSeries = Literal[
    "SWESTR",
    "SWESTRAVG1W",
    "SWESTRAVG1M",
    "SWESTRAVG2M",
    "SWESTRAVG3M",
    "SWESTRAVG6M",
    "SWESTRINDEX",
]


class RiksbankSwestrFetchParams(BaseModel):
    """Parameters for fetching a SWESTR fixing / compounded average / index.

    ``series`` is a closed enum of the seven SWESTR identifiers that
    Riksbank publishes today. Routing per identifier type:

    * ``SWESTR``         → ``/all/SWESTR`` (raw daily fixing, rate only)
    * ``SWESTRAVG*``     → ``/avg/<id>`` (compounded average — rate, start/end)
    * ``SWESTRINDEX``    → ``/index/<id>`` (published as an index value)

    ``from_date``/``to_date`` together request a window; omit both for
    the latest observation, which hits ``/latest/<id>``,
    ``/avg/latest/<id>``, or ``/index/latest/<id>`` respectively.
    """

    series: Annotated[SwestrSeries, "ns:riksbank"] = Field(
        ...,
        description=(
            "SWESTR identifier (one of: SWESTR, SWESTRAVG1W, SWESTRAVG1M, "
            "SWESTRAVG2M, SWESTRAVG3M, SWESTRAVG6M, SWESTRINDEX)."
        ),
    )
    from_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    to_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @model_validator(mode="after")
    def _both_dates_or_neither(self) -> RiksbankSwestrFetchParams:
        # ``from_date`` alone is ambiguous against /all/<id> which treats
        # fromDate alone as "everything since". We require both for
        # deterministic window semantics matching the docs. A model-level
        # validator fires regardless of whether ``to_date`` was set,
        # unlike a ``field_validator`` which silently skips when the
        # field takes its None default.
        if (self.from_date is None) != (self.to_date is None):
            raise ValueError("Provide both from_date and to_date, or neither")
        return self


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

RIKSBANK_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` is the upstream long-form text. Routing it
        # through DESCRIPTION (not METADATA) lifts it into
        # ``semantic_text()`` so the embedder indexes the phrase itself,
        # in addition to BM25.
        Column(name="description", role=ColumnRole.DESCRIPTION),
        # ``source`` tells the agent which fetch connector to call. Every
        # row currently emits ``"swea"``; a future CBA fetcher would emit
        # ``"cba"``. Without this, agents would have to sniff the
        # ``series_id`` prefix.
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
        Column(name="series_id", role=ColumnRole.KEY, param_key="series_id", namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

# SWESTR payloads carry more metadata than a plain rate: a publication
# time, transaction volume / count / agent count (for the raw fixing),
# and 12.5/87.5 percentiles of the underlying trades. Only ``date`` and
# ``value`` are declared as named DATA columns here — the rest ride
# along as additional columns so agents with SWESTR-specific analyses
# can read them without a fetch-level schema migration.
RIKSBANK_SWESTR_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series", role=ColumnRole.KEY, param_key="series", namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http(api_key: str = "") -> HttpClient:
    headers: dict[str, str] = {}
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return HttpClient(_BASE_URL, headers=headers)


def _make_swestr_http(api_key: str = "") -> HttpClient:
    headers: dict[str, str] = {}
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key
    return HttpClient(_SWESTR_BASE_URL, headers=headers)


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


def _swestr_parse_rows(series: str, payload: Any) -> list[dict[str, Any]]:
    """Flatten a SWESTR response into ``{series, date, value, ...}`` rows.

    The ``/latest`` endpoints return a single JSON object; the windowed
    endpoints return a JSON list. Raw SWESTR and the averages carry a
    ``rate`` field; the index uses ``value`` instead. We normalise both
    onto a single ``value`` column so downstream code doesn't branch on
    series kind, while preserving the native field name on each row for
    visibility into why a value is what it is.
    """
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        date_val = item.get("date") or item.get("Date")
        if date_val is None:
            continue
        # Index payloads use ``value`` (an index level); rate payloads
        # use ``rate``. Defensive lookups tolerate either being missing.
        raw_value = item.get("rate")
        if raw_value is None:
            raw_value = item.get("value")
        try:
            value = float(raw_value) if raw_value not in (None, "", "NaN") else None  # type: ignore[arg-type]
        except (ValueError, TypeError):
            value = None
        row: dict[str, Any] = {
            "series": series,
            "date": date_val,
            "value": value,
        }
        # Pass through Riksbank's confidence/volume metadata — useful for
        # analysts spotting alternative-calculation days. The ``avg``
        # endpoint carries ``startDate`` (the window start); the raw
        # fixing carries ``volume`` / ``numberOfTransactions`` etc.
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
# ``PMI``/``PMD`` for daily fix, ``PMM``/``PMW`` for weekly/monthly,
# ``PMQ`` quarterly, ``PMA`` annual. We record both the inferred value
# and how we got it (``frequency_source``) so downstream consumers can
# decide whether to trust it.
_FREQ_BY_SUFFIX: tuple[tuple[str, str], ...] = (
    ("PMI", "Daily"),
    ("PMD", "Daily"),
    ("PMW", "Weekly"),
    ("PMM", "Monthly"),
    ("PMQ", "Quarterly"),
    ("PMA", "Annual"),
)

# Group-id → frequency mapping for buckets where the SWEA hierarchy
# pins down the publication cadence. Mapped against the live Groups
# tree (probed 2026-04 via /swea/v1/Groups). The Riksbank publishes
# every interest rate and FX series in the daily groups at 16:15 CET;
# monthly/annual aggregate buckets carry their own group ids (133, 134)
# whose children inherit the cadence.
_FREQ_BY_GROUP_ID: dict[int, str] = {
    # Riksbank policy rates and the historic discount/reference rate.
    2: "Daily",  # Riksbank key interest rates
    3: "Daily",  # Other Riksbank interest rates
    # Swedish market (based) rates — daily fixings.
    5: "Daily",  # STIBOR
    6: "Daily",  # Swedish Treasury Bills (SE TB)
    7: "Daily",  # Swedish Government Bonds (SE GVB)
    8: "Daily",  # Swedish Fixing Rates (SE STFIX)
    9: "Daily",  # Swedish Mortgage Bonds (SE MB)
    10: "Daily",  # Swedish Commercial Paper (SE CP)
    # International market rates — daily benchmarks aggregated by Riksbank.
    97: "Daily",  # Euro Market Rates, 3 months
    98: "Daily",  # Euro Market Rates, 6 months
    99: "Daily",  # International Government Bonds, 5 years
    100: "Daily",  # International Government Bonds, 10 years
    # Exchange-rate buckets (kronor cross rates and indices).
    12: "Daily",  # Swedish TCW index
    130: "Daily",  # Currencies against Swedish kronor
    131: "Daily",  # Cross rates
    138: "Daily",  # Special Drawing Rights (SDR)
    151: "Daily",  # Swedish KIX index
    155: "Daily",  # Forward Premiums
    # Aggregations — group cadence is the value, not the publication frequency.
    133: "Monthly",  # Monthly aggregate
    134: "Annual",  # Annual aggregate
}


def _infer_frequency(series_id: str, group_id: int | None) -> tuple[str, str]:
    """Best-effort frequency for a SWEA series.

    Returns ``(frequency, source)`` where ``source`` is one of
    ``"group"``, ``"suffix"``, or ``"unknown"`` so downstream consumers
    can tell apart a confident value from a heuristic.
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

    The endpoint returns a single root node with ``childGroups``; each
    node carries ``groupId``, ``name``, ``description``. Earlier code
    looked for ``groupInfos``/``groupName``/``children`` keys that the
    API does not emit, so groups always came back empty. We also accept
    the legacy keys defensively in case the API surface ever changes.
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
    ``midDescription`` (one-paragraph), ``shortDescription`` (label).
    SWEA always populates at least one of the three, but we synthesise
    a fall-back from id + group name + provider so the DESCRIPTION
    column is never empty (an empty DESCRIPTION still indexes via BM25
    but contributes nothing to the embedder).
    """
    for key in ("longDescription", "midDescription", "shortDescription"):
        v = series.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    sid = series.get("seriesId") or "series"
    provider = series.get("source") or "Sveriges Riksbank"
    return f"{sid} — published by {provider}."


# ---------------------------------------------------------------------------
# SWESTR static registry
#
# SWESTR is not in SWEA's ``/Series`` — it lives on a separate API with
# its own URL scheme (``/latest/<id>``, ``/avg/latest/<id>``,
# ``/index/latest/<id>``). The set of series is small (seven) and
# stable: the fixing, five compounded averages, and one index. A static
# registry is the right surface here — it spares the enumerator a
# second live API dependency for data that doesn't change.
# ---------------------------------------------------------------------------

_SWESTR_PROVIDER = "Sveriges Riksbank"
_SWESTR_GROUP = "SWESTR > Swedish Krona Short-Term Rate"
_SWESTR_INCEPTION_DATE = "2021-09-01"  # Official launch of published values.

#: SWESTR series definitions. ``kind`` drives which URL family
#: :func:`riksbank_swestr_fetch` hits (``rate`` → ``/all`` or
#: ``/latest``; ``avg`` → ``/avg``; ``index`` → ``/index``).
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


def _build_swestr_rows() -> list[dict[str, Any]]:
    """One catalog row per :data:`_SWESTR_SERIES` entry.

    Pure function — the registry is static so this involves no I/O.
    Each row carries ``source="swestr"`` so a dispatching agent routes
    the hit to :func:`riksbank_swestr_fetch` rather than the SWEA
    :func:`riksbank_fetch` connector.
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
    # weirder we leave to the embedder/BM25 as a literal — frequencies of
    # malformed dates have never been observed in practice.
    return s[:10]


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(env={"api_key": "RIKSBANK_API_KEY"}, output=RIKSBANK_FETCH_OUTPUT, tags=["macro", "se"])
async def riksbank_fetch(params: RiksbankFetchParams, *, api_key: str = "") -> Result:
    """Fetch a single Riksbank SWEA time series by series_id.

    Returns date + value with the upstream series name as title. Use
    ``from_date``/``to_date`` together to fetch a window; omit both to
    receive the latest observation.
    """
    http = _make_http(api_key)

    if params.from_date and params.to_date:
        path = f"/Observations/{params.series_id}/{params.from_date}/{params.to_date}"
    else:
        path = f"/Observations/Latest/{params.series_id}"

    response = await http.request("GET", path)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="riksbank", op_name="Observations")
    data = response.json()

    # Resolve series title from /Series endpoint
    title = params.series_id
    try:
        series_resp = await http.request("GET", "/Series")
        if series_resp.status_code == 200:
            series_list = series_resp.json()
            if isinstance(series_list, dict):
                series_list = [series_list]
            for s in series_list:
                sid = s.get("seriesId", s.get("id", ""))
                if sid == params.series_id:
                    title = s.get("seriesName", s.get("name", s.get("shortDescription", params.series_id)))
                    break
    except Exception:
        logger.debug("Could not resolve title for %s, using series_id", params.series_id)

    items = data if isinstance(data, list) else [data]
    rows: list[dict[str, Any]] = []
    for item in items:
        date_val = item.get("date") or item.get("Date")
        raw_value = item.get("value") or item.get("Value")
        if date_val is None:
            continue
        try:
            value = float(raw_value) if raw_value not in (None, "", "NaN") else None
        except (ValueError, TypeError):
            value = None
        rows.append(
            {
                "series_id": params.series_id,
                "title": title,
                "date": date_val,
                "value": value,
            }
        )

    if not rows:
        raise EmptyDataError(provider="riksbank", message=f"No observations returned for: {params.series_id}")

    return Result.from_dataframe(
        pd.DataFrame(rows),
        Provenance(
            source="riksbank",
            params={"series_id": params.series_id},
            properties={"source_url": "https://www.riksbank.se/en-gb/statistics/"},
        ),
    )


@connector(
    env={"api_key": "RIKSBANK_API_KEY"},
    output=RIKSBANK_SWESTR_FETCH_OUTPUT,
    tags=["macro", "se"],
)
async def riksbank_swestr_fetch(params: RiksbankSwestrFetchParams, *, api_key: str = "") -> Result:
    """Fetch a SWESTR fixing / compounded average / index series.

    Dispatches on :func:`_swestr_kind` across three URL families:

    * ``rate``  → ``/latest/SWESTR`` or ``/all/SWESTR``
    * ``avg``   → ``/avg/latest/<id>`` or ``/avg/<id>``
    * ``index`` → ``/index/latest/<id>`` or ``/index/<id>``

    ``from_date``/``to_date`` together request a window; omit both for
    the latest published value (one-row result). Returns date + value
    plus SWESTR's native metadata (publication time, percentiles,
    transaction volumes) on additional columns.
    """
    http = _make_swestr_http(api_key)
    kind = _swestr_kind(params.series)

    if params.from_date and params.to_date:
        # Windowed endpoints use ``fromDate``/``toDate`` query params.
        query = {"fromDate": params.from_date, "toDate": params.to_date}
        if kind == "rate":
            path = f"/all/{params.series}"
        elif kind == "avg":
            path = f"/avg/{params.series}"
        else:  # index
            path = f"/index/{params.series}"
    else:
        query = None
        if kind == "rate":
            path = f"/latest/{params.series}"
        elif kind == "avg":
            path = f"/avg/latest/{params.series}"
        else:  # index
            path = f"/index/latest/{params.series}"

    op_name = path.lstrip("/")
    response = await http.request("GET", path, params=query)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="riksbank", op_name=op_name)
    payload = response.json()

    rows = _swestr_parse_rows(params.series, payload)
    if not rows:
        raise EmptyDataError(
            provider="riksbank",
            message=f"No SWESTR observations returned for series={params.series!r}",
        )

    # Resolve a human-readable title from the static registry so the
    # agent-facing output matches what the catalog row advertises.
    title: str = params.series
    for spec in _SWESTR_SERIES:
        if spec["series_id"] == params.series:
            title = spec["title"]
            break
    df = pd.DataFrame(rows)
    df["title"] = title

    return Result.from_dataframe(
        df,
        Provenance(
            source="riksbank",
            params={"series": params.series},
            properties={"source_url": "https://www.riksbank.se/en-gb/statistics/swestr/"},
        ),
    )


@enumerator(
    env={"api_key": "RIKSBANK_API_KEY"},
    output=RIKSBANK_ENUMERATE_OUTPUT,
    tags=["macro", "se"],
)
async def enumerate_riksbank(params: RiksbankEnumerateParams, *, api_key: str = "") -> pd.DataFrame:
    """Enumerate every Riksbank time series available over the SWEA API.

    Two upstream calls — ``/Groups`` (a hierarchy of categorisation
    buckets) and ``/Series`` (one entry per series with descriptions and
    observation date span). Each output row is one (series_id, group)
    pair with rich description, frequency, and date-range metadata.

    SWEA rows carry ``source="swea"``. SWESTR rows (the overnight
    fixing, five compounded averages, one index) are appended from a
    static registry and carry ``source="swestr"`` — the registry is
    the right surface for that family because the series set is small
    (seven), stable, and the live SWESTR endpoints return observations
    rather than metadata (there is no ``/Series`` equivalent). Agents
    route SWESTR hits to :func:`riksbank_swestr_fetch`.

    A third family — forecasts/outcomes at
    ``api.riksbank.se/forecasts/v1`` — is documented by Riksbank but
    every probed path returns ``404 Resource not found`` as of the
    catalog freeze date (third-party clients that target the same URL
    scheme carry in-code comments warning the API "may be temporarily
    unavailable"). Forecasts are therefore *not* catalogued: emitting
    rows for 404-ing endpoints would mislead dispatching agents. When
    Riksbank re-opens the forecasts endpoint, this enumerator extends
    to emit additional rows with ``source="forecasts"`` and a sibling
    ``riksbank_forecasts_fetch`` connector lands here — the
    ``source`` dispatch contract is already in place.

    A fourth family was investigated under the working name **CBA**
    (Central Bank Asset), but no corresponding endpoint exists on
    ``api.riksbank.se``: probes against ``/cba``, ``/cba/v1/...``,
    ``/cba/Series``, ``/cba/CrossRates`` etc. all return
    ``404 Resource not found``, and the developer portal SPA does not
    expose a machine-readable product index. CBA is not implemented.
    """
    http = _make_http(api_key)

    # ``/Groups`` returns a single root node (not a list). We tolerate both
    # for safety and walk ``childGroups`` (the actual upstream key — earlier
    # versions of this enumerator looked for ``groupInfos`` and silently lost
    # all group context).
    groups_resp = await http.request("GET", "/Groups")
    try:
        groups_resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="riksbank", op_name="Groups")
    groups_data = groups_resp.json()
    group_lookup = _flatten_groups(groups_data)

    series_resp = await http.request("GET", "/Series")
    try:
        series_resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="riksbank", op_name="Series")
    series_data = series_resp.json()
    if isinstance(series_data, dict):
        series_data = [series_data]

    rows: list[dict[str, Any]] = []
    for s in series_data:
        sid = s.get("seriesId", s.get("id", ""))
        if not sid:
            continue
        group_id_raw = s.get("groupId", s.get("group", ""))
        group_id_int: int | None
        try:
            group_id_int = int(group_id_raw) if group_id_raw != "" else None
        except (TypeError, ValueError):
            group_id_int = None
        frequency, frequency_source = _infer_frequency(sid, group_id_int)

        # Title prefers the explicit ``shortDescription`` label (which is
        # what SWEA shows in its UI) and falls back through the other
        # description fields, finally to the id itself. The legacy
        # ``seriesName``/``name`` keys are accepted for forward-compat.
        title = (
            s.get("shortDescription")
            or s.get("seriesName")
            or s.get("name")
            or s.get("midDescription")
            or sid
        )

        rows.append(
            {
                "series_id": sid,
                "title": str(title).strip() or sid,
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

    # Append SWESTR rows from the static registry. These series are
    # published over a separate URL family (/latest, /avg, /index)
    # rather than SWEA /Series, so a registry — not a second live
    # upstream call — is the right surface for the catalog.
    rows.extend(_build_swestr_rows())

    columns = [
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
    ]
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_riksbank.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    PARSIMONY_RIKSBANK_CATALOG_URL_ENV,
    RIKSBANK_SEARCH_OUTPUT,
    RiksbankSearchParams,
    riksbank_search,
)

CATALOGS: list[tuple[str, object]] = [("riksbank", enumerate_riksbank)]

CONNECTORS = Connectors([riksbank_fetch, riksbank_swestr_fetch, enumerate_riksbank, riksbank_search])
