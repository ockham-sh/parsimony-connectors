"""SNB data fetch — resolves a cube_id across the publication and warehouse APIs.

A search hit carries a compound ``code`` = ``{cube_id}#{series_key}``; the agent
passes the ``cube_id`` portion to ``snb_fetch``. ``snb_fetch`` routes by id shape
so **every** catalogued cube is fetchable:

* a bare id (``rendoblim``) → ``/api/cube/{id}/data/csv/{lang}`` (publication);
* an SDMX id with ``@`` (``BSTA@SNB.AUR_U.ODF``) → ``/api/warehouse/cube/{id}/data/csv/{lang}``
  with the id's ``@`` mapped to ``.`` (warehouse).
"""

from __future__ import annotations

from typing import Annotated

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError

from parsimony_snb import _http, parsing
from parsimony_snb.outputs import SNB_FETCH_OUTPUT


@connector(output=SNB_FETCH_OUTPUT, tags=["macro", "ch"])
def snb_fetch(
    cube_id: Annotated[str, Namespace("snb")],
    from_date: str | None = None,
    to_date: str | None = None,
    dim_sel: str | None = None,
    lang: str = "en",
) -> pd.DataFrame:
    """Fetch an SNB cube as a long-format time series by cube_id.

    Handles both publication cubes (bare ids like ``rendoblim``, ``devkum``) and
    data-warehouse cubes (SDMX ids like ``BSTA@SNB.AUR_U.ODF``) — routing is
    automatic from the id shape. Returns a ``date`` column, the cube's string
    dimension code columns, and a numeric ``Value`` column, stamped with
    ``cube_id`` and the cube ``title``. Optional ``from_date``/``to_date`` (YYYY,
    YYYY-MM, or YYYY-MM-DD), ``dim_sel`` (e.g. ``D0(V0,V1)``), and ``lang``
    (en/de/fr/it) pass through to the portal.
    """
    cube_id = cube_id.strip()
    if not cube_id:
        raise InvalidParameterError("snb", "cube_id must be non-empty")

    http = _http.client()

    req_params: dict[str, str] = {}
    if from_date:
        req_params["fromDate"] = from_date
    if to_date:
        req_params["toDate"] = to_date
    if dim_sel:
        req_params["dimSel"] = dim_sel

    text = _http.get_text(
        http,
        _http.cube_data_path(cube_id, lang=lang),
        op_name="cube/data",
        params=req_params or None,
    )

    df = parsing.parse_snb_csv(text, cube_id)
    if df.empty:
        raise EmptyDataError(
            "snb",
            message=f"No data returned for cube: {cube_id}",
            query_params={"cube_id": cube_id, "from_date": from_date, "to_date": to_date, "dim_sel": dim_sel},
        )

    df["date"] = pd.to_datetime(df["date"])
    df["cube_id"] = cube_id
    # The /api payloads carry no cube title; resolve it best-effort from the portal
    # getCubeInfo metadata (falls back to the cube_id if the call fails).
    info = _http.get_cube_info(http, cube_id, lang=lang)
    df["title"] = str((info or {}).get("title") or "").strip() or cube_id
    return df
