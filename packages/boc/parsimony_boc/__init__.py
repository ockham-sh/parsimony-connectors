"""Bank of Canada (BoC): fetch + catalog enumeration.

API base: ``https://www.bankofcanada.ca/valet`` (Valet). No authentication
required (keyless public JSON API — no ``secrets=``/``bind()``/
``UnauthorizedError``; ``load()`` binds only the catalog URL for search).

Transport:

* ``boc_fetch`` (one per-call request) uses the canonical core helper pair
  ``make_http_client`` + ``fetch_json`` — GET + ``raise_for_status`` +
  ``map_http_error`` / ``map_timeout_error`` + ``json()`` + ``None``-param
  dropping, all in one call (Valet is JSON, so ``fetch_json`` fits).
* ``enumerate_boc`` keeps its own hand-rolled, concurrency-capped group
  fan-out (it does NOT use ``parsimony_shared``), but builds the client via
  ``make_http_client`` + ``pooled_client`` and maps errors/timeouts through
  the kernel helpers. The list endpoints map through ``fetch_json``; the
  best-effort per-group membership fetch swallows transient errors by design
  (BoC leaves dead group IDs in the index).

Endpoints used:

* ``GET /observations/{names}/json`` and
  ``GET /observations/group/{name}/json`` — time-series observations.
* ``GET /lists/series/json`` (~15.6k series) and ``GET /lists/groups/json``
  (~2.4k groups) — catalog index.
* ``GET /groups/{name}/json`` — per-group series membership (the fan-out).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
)
from parsimony.transport import HttpClient, pooled_client
from parsimony.transport.helpers import fetch_json, make_http_client

logger = logging.getLogger(__name__)


_BASE_URL = "https://www.bankofcanada.ca/valet"

#: Concurrency cap for the per-group fan-out used to build the
#: series→group map. BoC's Valet endpoint is unauthenticated and
#: tolerates moderate concurrency; 16 keeps total enumeration time at
#: ~1 minute for the ~2,400 groups while staying well under any sensible
#: rate limit.
_GROUP_FETCH_CONCURRENCY = 16


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BOC_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # The KEY is either a series name (e.g. ``FXUSDCAD``) or a group
        # entry prefixed with ``group:`` (e.g. ``group:FX_RATES_DAILY``).
        # Groups are first-class addressable entities — ``boc_fetch``
        # accepts ``series_name="group:NAME"`` and BoC's
        # ``/observations/group/{name}/json`` returns the full panel — so
        # they get their own catalog rows for discovery, in addition to
        # the per-series rows. The ``group:`` prefix matches the syntax
        # ``boc_fetch`` already expects.
        Column(name="series_name", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` is the upstream Valet ``description`` text. It is
        # ordinary metadata; catalogs decide explicitly whether to index it.
        # For group rows this carries the group's ``description`` text from
        # ``/lists/groups/json`` (e.g. units and frequency hints like
        # "Month-end, Millions of dollars").
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` tells the agent which fetch connector to call. BOC has
        # a single Valet source today; the column is future-proofing for a
        # parallel source so dispatch is already wired (matches Treasury's
        # ``fiscal_data``/``treasury_rates`` split).
        Column(name="source", role=ColumnRole.METADATA),
        # ``entity_type`` is ``"series"`` for individual series rows and
        # ``"group"`` for group rows — lets agents filter or weight by
        # entity granularity.
        Column(name="entity_type", role=ColumnRole.METADATA),
        # ``group`` carries the upstream group ID (e.g. ``FX_RATES_DAILY``)
        # the series belongs to — populated from /lists/groups/json plus
        # per-group membership. Multi-group membership is rare; when it
        # occurs we keep the first encountered group ID. Empty string when
        # a series isn't a member of any catalogued group. For group rows
        # this is the group's own ID.
        Column(name="group", role=ColumnRole.METADATA),
        Column(name="group_label", role=ColumnRole.METADATA),
    ]
)

BOC_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_name", role=ColumnRole.KEY, param_key="series_name", namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

_ENUMERATE_COLUMNS: tuple[str, ...] = (
    "series_name",
    "title",
    "description",
    "source",
    "entity_type",
    "group",
    "group_label",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_observations(
    json_data: dict[str, Any],
    series_details: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Parse BoC Valet API observations response into long-format DataFrame.

    The observations array has entries like:
    {"d": "2024-01-15", "FXUSDCAD": {"v": "1.3456"}, "FXEURCAD": {"v": "1.4678"}}
    """
    observations = json_data.get("observations", [])
    if not observations:
        return pd.DataFrame(columns=["series_name", "title", "date", "value"])

    # Discover series columns (everything except "d")
    sample = observations[0]
    series_cols = [k for k in sample if k != "d"]

    rows: list[dict[str, Any]] = []
    for obs in observations:
        date = obs.get("d", "")
        for col in series_cols:
            raw = obs.get(col)
            if raw is None:
                continue
            raw_value = raw.get("v") if isinstance(raw, dict) else raw
            try:
                value = float(raw_value) if raw_value is not None and raw_value not in ("", "NaN") else None
            except (ValueError, TypeError):
                value = None

            # Resolve title from seriesDetail if available
            title = col
            if series_details and col in series_details:
                detail = series_details[col]
                title = detail.get("label", detail.get("description", col))

            rows.append(
                {
                    "series_name": col,
                    "title": title,
                    "date": date,
                    "value": value,
                }
            )

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["series_name", "title", "date", "value"])


async def _list_groups(client: HttpClient) -> dict[str, dict[str, Any]]:
    """Return BoC's group index (~2.4k entries) from ``/lists/groups/json``.

    This is the **bounding seam** for live tests: monkeypatch this module
    global to return a 2–3 group slice so the per-group fan-out fires a
    handful of requests, never the full ~2,400-request crawl. The connector
    reads it as a module global at call time, so the monkeypatch takes.
    """
    payload = await fetch_json(client, path="lists/groups/json", provider="boc", op_name="groups/list")
    if not isinstance(payload, dict):
        raise ParseError("boc", "unexpected /lists/groups/json shape (expected object)")
    groups = payload.get("groups") or {}
    return groups if isinstance(groups, dict) else {}


async def _fetch_group_membership(
    client: HttpClient,
    group_name: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list[str]]:
    """Fetch a single group's series-membership list.

    Returns ``(group_name, list_of_series_names)``. On any HTTP error the
    group is treated as empty — enumeration continues. We don't surface
    individual group failures because cataloguing is a best-effort sweep
    and BoC does occasionally retire group endpoints while leaving them
    in the index.
    """
    async with semaphore:
        try:
            resp = await client.request("GET", f"/groups/{group_name}/json")
            resp.raise_for_status()
            body = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            # Best-effort: a transport failure OR a 200-with-non-JSON body
            # (BoC sometimes serves an HTML stub for a retired-but-indexed
            # group) treats this group as empty so the sweep continues.
            logger.warning("BoC group fetch failed for %r: %s", group_name, exc)
            return group_name, []

    if not isinstance(body, dict):
        return group_name, []
    details = body.get("groupDetails") or {}
    members = details.get("groupSeries") or {}
    if not isinstance(members, dict):
        return group_name, []
    return group_name, [s for s in members if s]


async def _build_series_to_group_map(
    client: HttpClient,
    groups_index: dict[str, dict[str, Any]],
) -> dict[str, tuple[str, str]]:
    """For each series, resolve ``(group_id, group_label)``.

    Multi-group membership is rare in BoC's catalog (groups partition
    series by economic theme); when it occurs the first encountered
    group wins. Iteration order is the order BoC returns groups in
    ``/lists/groups/json``, which is stable across requests.

    Connections are pooled across the fan-out via :func:`pooled_client`.
    """
    semaphore = asyncio.Semaphore(_GROUP_FETCH_CONCURRENCY)
    async with pooled_client(client) as shared:
        tasks = [_fetch_group_membership(shared, group_name, semaphore) for group_name in groups_index]
        results = await asyncio.gather(*tasks)

    series_to_group: dict[str, tuple[str, str]] = {}
    for group_name, members in results:
        info = groups_index.get(group_name) or {}
        label = info.get("label") if isinstance(info, dict) else ""
        label = label or ""
        for series_name in members:
            if series_name not in series_to_group:
                series_to_group[series_name] = (group_name, label)
    return series_to_group


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BOC_FETCH_OUTPUT, tags=["macro", "ca"])
async def boc_fetch(
    series_name: Annotated[str, "ns:boc"],
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Fetch Bank of Canada time series by series name(s) or group name.

    Use 'group:GROUP_NAME' syntax for group queries (e.g. group:FX_RATES_DAILY).
    Otherwise, pass comma-separated series names (e.g. FXUSDCAD,FXEURCAD).
    """
    series_name = series_name.strip()
    if not series_name:
        raise InvalidParameterError("boc", "series_name must be non-empty")

    if series_name.startswith("group:"):
        group_name = series_name[6:].strip()
        if not group_name:
            raise InvalidParameterError("boc", "group name must be non-empty after 'group:'")
        path = f"observations/group/{group_name}/json"
    else:
        path = f"observations/{series_name}/json"

    json_data = await fetch_json(
        make_http_client(_BASE_URL, timeout=60.0),
        path=path,
        params={"start_date": start_date, "end_date": end_date},
        provider="boc",
        op_name="observations",
    )
    if not isinstance(json_data, dict):
        raise ParseError("boc", f"unexpected observations response shape for: {series_name}")

    series_details = json_data.get("seriesDetail")
    df = _parse_observations(json_data, series_details)
    if df.empty:
        raise EmptyDataError(
            "boc",
            message=f"No observations returned for: {series_name}",
            query_params={"series_name": series_name, "start_date": start_date, "end_date": end_date},
        )

    return df


@enumerator(output=BOC_ENUMERATE_OUTPUT, tags=["macro", "ca"])
async def enumerate_boc() -> pd.DataFrame:
    """Enumerate every Bank of Canada series via Valet's three list endpoints.

    Granularity is one row per series — Valet addresses observations per
    series, so series-level keys are the right unit (~15k rows) — plus one
    row per group (keyed ``group:NAME``) so whole panels are discoverable.

    Pipeline: /lists/series/json and /lists/groups/json, then a concurrent
    /groups/{name}/json fan-out (one request per group) for series
    membership.
    """
    client = make_http_client(_BASE_URL, timeout=60.0)

    series_payload = await fetch_json(client, path="lists/series/json", provider="boc", op_name="series/list")
    if not isinstance(series_payload, dict):
        raise ParseError("boc", "unexpected /lists/series/json shape (expected object)")

    groups_index = await _list_groups(client)
    series_to_group = await _build_series_to_group_map(client, groups_index)

    series = series_payload.get("series") or {}
    if not isinstance(series, dict):
        series = {}

    rows: list[dict[str, str]] = []
    for series_name, info in series.items():
        if not series_name:
            continue
        if isinstance(info, dict):
            label = info.get("label") or series_name
            desc = info.get("description") or ""
        else:
            label = str(info)
            desc = ""

        group_id, group_label = series_to_group.get(series_name, ("", ""))

        rows.append(
            {
                "series_name": series_name,
                "title": label,
                "description": desc,
                "source": "valet",
                "entity_type": "series",
                "group": group_id,
                "group_label": group_label,
            }
        )

    # Emit one row per group as a discoverable catalog entity. Groups are
    # addressable via ``boc_fetch(series_name="group:NAME")`` (BoC's
    # ``/observations/group/{name}/json``), so cataloguing them lets
    # agents search by group description (e.g. "Month-end, Millions of
    # dollars" — uniquely a group-level signal) and fetch a whole panel
    # in one shot. ~2.4k groups; ~2.2k carry non-empty descriptions in
    # practice. Group rows use the ``group:`` prefix in their KEY,
    # matching the syntax ``boc_fetch`` already accepts.
    for group_name, group_info in groups_index.items():
        if not group_name:
            continue
        if isinstance(group_info, dict):
            g_label = group_info.get("label") or group_name
            g_desc = group_info.get("description") or ""
        else:
            g_label = str(group_info)
            g_desc = ""
        rows.append(
            {
                "series_name": f"group:{group_name}",
                "title": g_label,
                "description": g_desc,
                "source": "valet",
                "entity_type": "group",
                "group": group_name,
                "group_label": g_label,
            }
        )

    columns = list(_ENUMERATE_COLUMNS)
    df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
    return df


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_boc.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    BOC_SEARCH_OUTPUT,
    PARSIMONY_BOC_CATALOG_URL_ENV,
    boc_search,
)

CONNECTORS = Connectors([boc_fetch, enumerate_boc, boc_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with optional catalog search defaults bound."""
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)
