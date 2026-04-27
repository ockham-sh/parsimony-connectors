"""Bank of Canada (BoC): fetch + catalog enumeration.

API docs: https://www.bankofcanada.ca/valet/docs
No authentication required.

"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any

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
from parsimony.transport import map_http_error
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


_BASE_URL = "https://www.bankofcanada.ca/valet"

#: Concurrency cap for the per-group fan-out used to build the
#: series→group map. BoC's Valet endpoint is unauthenticated and
#: tolerates moderate concurrency; 16 keeps total enumeration time at
#: ~1 minute for the ~2,350 groups while staying well under any sensible
#: rate limit.
_GROUP_FETCH_CONCURRENCY = 16


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BocFetchParams(BaseModel):
    """Parameters for fetching Bank of Canada time series."""

    series_name: Annotated[str, "ns:boc"] = Field(
        ...,
        description=(
            "Comma-separated BoC series names (e.g. FXUSDCAD,FXEURCAD) "
            "or a group name prefixed with 'group:' (e.g. group:FX_RATES_DAILY)"
        ),
    )
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")

    @field_validator("series_name")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("series_name must be non-empty")
        return v


class BocEnumerateParams(BaseModel):
    """No parameters needed — enumerates all BoC series."""

    pass


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
        # ``description`` is the upstream Valet ``description`` text — the
        # most useful semantic signal for retrieval. Routed via DESCRIPTION
        # (not METADATA) so it lifts into ``semantic_text()`` for the
        # embedder, mirroring how Treasury surfaces ``definition``. For
        # group rows this carries the group's ``description`` text from
        # ``/lists/groups/json`` (e.g. units and frequency hints like
        # "Month-end, Millions of dollars").
        Column(name="description", role=ColumnRole.DESCRIPTION),
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


async def _fetch_group_membership(
    client: httpx.AsyncClient,
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
            resp = await client.get(f"/groups/{group_name}/json")
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("BoC group fetch failed for %r: %s", group_name, exc)
            return group_name, []
        body = resp.json()

    details = body.get("groupDetails") or {}
    members = details.get("groupSeries") or {}
    if not isinstance(members, dict):
        return group_name, []
    return group_name, [s for s in members if s]


async def _build_series_to_group_map(
    client: httpx.AsyncClient,
    groups_index: dict[str, dict[str, Any]],
) -> dict[str, tuple[str, str]]:
    """For each series, resolve ``(group_id, group_label)``.

    Multi-group membership is rare in BoC's catalog (groups partition
    series by economic theme); when it occurs the first encountered
    group wins. Iteration order is the order BoC returns groups in
    ``/lists/groups/json``, which is stable across requests.
    """
    semaphore = asyncio.Semaphore(_GROUP_FETCH_CONCURRENCY)
    tasks = [
        _fetch_group_membership(client, group_name, semaphore)
        for group_name in groups_index
    ]
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
async def boc_fetch(params: BocFetchParams) -> Result:
    """Fetch Bank of Canada time series by series name(s) or group name.

    Use 'group:GROUP_NAME' syntax for group queries (e.g. group:FX_RATES_DAILY).
    Otherwise, pass comma-separated series names (e.g. FXUSDCAD,FXEURCAD).
    """
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=60.0) as client:
        req_params: dict[str, str] = {}
        if params.start_date:
            req_params["start_date"] = params.start_date
        if params.end_date:
            req_params["end_date"] = params.end_date

        if params.series_name.startswith("group:"):
            group_name = params.series_name[6:].strip()
            url = f"/observations/group/{group_name}/json"
        else:
            url = f"/observations/{params.series_name}/json"

        response = await client.get(url, params=req_params)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="boc", op_name="observations")
        json_data = response.json()

    series_details = json_data.get("seriesDetail")
    df = _parse_observations(json_data, series_details)
    if df.empty:
        raise EmptyDataError(provider="boc", message=f"No observations returned for: {params.series_name}")

    return Result.from_dataframe(
        df,
        Provenance(
            source="boc",
            params={"series_name": params.series_name},
            properties={"source_url": "https://www.bankofcanada.ca/valet/docs"},
        ),
    )


@enumerator(
    output=BOC_ENUMERATE_OUTPUT,
    tags=["macro", "ca"],
)
async def enumerate_boc(params: BocEnumerateParams) -> pd.DataFrame:
    """Enumerate every Bank of Canada series via Valet's three list endpoints.

    Granularity is one row per series — Valet addresses observations per
    series, so series-level keys are the right unit (~15k rows).

    Pipeline:

    1. ``/lists/series/json`` — single call returning all ~15k series with
       upstream ``label`` and ``description``. ``description`` lands in a
       ColumnRole.DESCRIPTION column so the embedder indexes it.
    2. ``/lists/groups/json`` — single call returning all ~2.3k groups
       with their labels.
    3. ``/groups/{name}/json`` — fanned out concurrently for every group
       to discover series membership; a series→group map is built and
       attached to every row. Groups exist purely for discovery; missing
       membership leaves the ``group`` field empty.
    """
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=60.0) as client:
        series_resp = await client.get("/lists/series/json")
        try:
            series_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="boc", op_name="series/list")
        series_payload = series_resp.json()

        groups_resp = await client.get("/lists/groups/json")
        try:
            groups_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="boc", op_name="groups/list")
        groups_payload = groups_resp.json()

        groups_index = groups_payload.get("groups") or {}
        if not isinstance(groups_index, dict):
            groups_index = {}
        series_to_group = await _build_series_to_group_map(client, groups_index)

    series = series_payload.get("series") or {}
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
    # in one shot. ~2.3k groups; 2.2k carry non-empty descriptions in
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

    columns = [
        "series_name",
        "title",
        "description",
        "source",
        "entity_type",
        "group",
        "group_label",
    ]
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_boc.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    BOC_SEARCH_OUTPUT,
    PARSIMONY_BOC_CATALOG_URL_ENV,
    BocSearchParams,
    boc_search,
)

CATALOGS: list[tuple[str, object]] = [("boc", enumerate_boc)]

CONNECTORS = Connectors([boc_fetch, enumerate_boc, boc_search])
