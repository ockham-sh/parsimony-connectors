"""EIA data-fetch connectors.

Three verbs over the EIA v2 API:

* ``eia_fetch`` — fetch a dataset by its route path (``petroleum/pri/spt``) with
  an optional measure, facet filters and a date window.
* ``eia_fetch_series`` — fetch by a legacy APIv1 series id (``PET.RWTC.D``,
  ``ELEC.SALES.CO-RES.A``) via the ``/v2/seriesid/{id}`` path. This addressing
  scheme lives **outside** the route tree the catalog enumerates, so it is the
  only connector that reaches a famous series straight from its well-known id.
* ``eia_facets`` — list the valid values of one facet dimension, so a fetch can
  be narrowed to a specific series (essential on huge datasets).

**Pagination is mandatory, not optional.** EIA caps every ``/data`` (and
``/seriesid``) response at 5,000 rows; a dataset like ``petroleum/pri/spt`` daily
has 91,285 rows and ``electricity/rto/region-data`` hourly has ~18.7M. The
single-page predecessor silently returned the first 5,000 of whatever matched.
Both fetch verbs read ``response.total`` and page through with ``offset`` until
they have it all — guarded by a pre-pagination row-count ceiling that raises an
actionable ``InvalidParameterError`` (echoing EIA's own "constrain with facet,
start, end" guidance) so an unbounded request fails loud instead of truncating.
"""

from __future__ import annotations

import re
from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError
from parsimony.transport import HttpClient

from parsimony_eia._http import PAGE_SIZE, eia_get, make_eia_client
from parsimony_eia.outputs import EIA_FACETS_OUTPUT, EIA_FETCH_OUTPUT, EIA_SERIES_OUTPUT

# A single fetch is bounded to this many rows. Above it, EIA's universe is too
# large to pull whole (electricity hourly alone is ~18.7M rows); we refuse with
# an actionable narrowing message rather than either truncate silently (the bug
# we are fixing) or page through millions of rows. ~60 pages.
MAX_FETCH_ROWS = 300_000


def _to_int(value: Any) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _facet_params(facets: dict[str, Any] | None) -> dict[str, Any]:
    """Render a ``{facet_id: value | [values]}`` dict into EIA query params.

    ``{"duoarea": "NUS", "product": ["EPCBRENT", "EPD2DC"]}`` →
    ``facets[duoarea][]=NUS`` and a repeated ``facets[product][]=...``. httpx
    expands a list-valued param into repeated keys.
    """
    out: dict[str, Any] = {}
    for raw_id, val in (facets or {}).items():
        fid = str(raw_id).strip()
        if not fid:
            continue
        key = f"facets[{fid}][]"
        if isinstance(val, (list, tuple, set)):
            out[key] = [str(v) for v in val]
        else:
            out[key] = str(val)
    return out


def _normalize_periods(raw: pd.Series) -> pd.Series:
    """Coerce EIA period strings (any frequency) to datetimes.

    EIA period shapes by frequency: ``2024`` (annual), ``2024-03`` (monthly),
    ``2024-03-15`` (daily/weekly), ``2025-Q4`` (quarterly), ``2026-06-10T07``
    (hourly), ``2026-06-10T03-04`` (local-hourly, trailing TZ band). pandas can
    parse all but the quarter form, which we expand to its quarter-start month,
    and the local-hourly TZ band, which we trim to the bare hour.
    """
    s = raw.astype("string").str.strip()
    s = s.str.replace(r"^(\d{4})-Q1$", r"\1-01-01", regex=True)
    s = s.str.replace(r"^(\d{4})-Q2$", r"\1-04-01", regex=True)
    s = s.str.replace(r"^(\d{4})-Q3$", r"\1-07-01", regex=True)
    s = s.str.replace(r"^(\d{4})-Q4$", r"\1-10-01", regex=True)
    s = s.str.replace(r"(T\d{2})-\d{2}$", r"\1", regex=True)
    return pd.to_datetime(s, errors="coerce", format="mixed")


def _detect_measure_col(df: pd.DataFrame) -> str | None:
    """Find the measure column when the caller didn't name one (seriesid path).

    EIA names the measure column inconsistently: petroleum uses ``value`` +
    ``units``; electricity uses ``sales``/``price``/... + a ``<measure>-units``
    sibling. Prefer ``value``; otherwise the column with a ``{col}-units`` twin.
    """
    if "value" in df.columns:
        return "value"
    for col in df.columns:
        if isinstance(col, str) and f"{col}-units" in df.columns:
            return col
    return None


_DUP_LABEL_RE = re.compile(r"-(name|units|description)$")


def _natural_key_columns(df: pd.DataFrame) -> list[str]:
    """Dimensional columns that identify a row (for boundary-dup-safe dedup).

    EIA offset pagination over an unsorted result is lossless but can repeat a
    row at a page boundary, so we dedup on the dimensional key: ``period`` plus
    the facet code columns (dropping the value, the ``*-units``/``*-name``/
    ``*-description`` label columns, and ``series-description``).
    """
    if "series" in df.columns and "period" in df.columns:
        return ["period", "series"]
    keys = [c for c in df.columns if isinstance(c, str) and c not in ("value", "units") and not _DUP_LABEL_RE.search(c)]
    return keys or list(df.columns)


def _shape_observations(
    data: list[dict[str, Any]],
    *,
    key_column: str,
    key_value: str,
    title: str,
    measure: str | None = None,
) -> pd.DataFrame:
    """Turn raw EIA rows into the long-format fetch frame.

    Normalizes ``period`` to datetime, the selected/detected measure to a
    ``value`` numeric column (coercing only that column so string facet metadata
    survives), dedups boundary-duplicate rows, and stamps the KEY + title. Every
    other EIA column (facet codes + their ``-name`` labels, ``series``, ``units``)
    folds in as DATA so a multi-series fetch stays disambiguated.
    """
    df = pd.DataFrame(data)
    if "period" in df.columns:
        df["period"] = _normalize_periods(df["period"])
    else:
        df["period"] = pd.NaT

    measure_col = measure if (measure and measure in df.columns) else _detect_measure_col(df)
    if measure_col and measure_col != "value":
        df = df.rename(columns={measure_col: "value"})
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    else:
        df["value"] = pd.NA

    df = df.drop_duplicates(subset=_natural_key_columns(df), keep="first").reset_index(drop=True)

    df[key_column] = key_value
    df["title"] = title
    return df


def _paginate(
    http: HttpClient,
    path: str,
    base_params: dict[str, Any],
    *,
    op_name: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Page through ALL rows for a /data or /seriesid request.

    Returns ``(rows, first_response)`` — the full row list plus the first page's
    response envelope (for its ``description``). Raises ``InvalidParameterError``
    before paging if the match exceeds ``MAX_FETCH_ROWS``.
    """
    first = eia_get(http, path, params={**base_params, "offset": 0, "length": PAGE_SIZE}, op_name=op_name)
    total = _to_int(first.get("total"))
    rows: list[dict[str, Any]] = list(first.get("data") or [])

    if total > MAX_FETCH_ROWS:
        raise InvalidParameterError(
            "eia",
            f"this request matches {total} rows, above the {MAX_FETCH_ROWS}-row fetch ceiling; "
            "narrow it with facets=, frequency=, start= or end= "
            "(EIA caps every page at 5000 rows).",
        )

    offset = len(rows)
    while offset < total:
        page = eia_get(http, path, params={**base_params, "offset": offset, "length": PAGE_SIZE}, op_name=op_name)
        chunk = list(page.get("data") or [])
        if not chunk:
            break
        rows.extend(chunk)
        offset += len(chunk)

    return rows, first


@connector(output=EIA_FETCH_OUTPUT, tags=["macro", "energy", "us"], secrets=("api_key",), requires=("EIA_API_KEY",))
def eia_fetch(
    route: Annotated[str, "ns:eia"],
    measure: str = "value",
    facets: dict[str, Any] | None = None,
    frequency: str | None = None,
    start: str | None = None,
    end: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch an EIA dataset by route path (e.g. ``petroleum/pri/spt``).

    ``measure`` selects EIA's required ``data[0]=`` facet — it is route-specific
    (``value`` for most; ``price``/``sales`` for electricity, ``heat-content`` for
    coal). ``facets`` is a ``{facet_id: value | [values]}`` dict (facet ids from the
    catalog, valid values from ``eia_facets``) that narrows the result to a series.
    ``frequency``, ``start``, and ``end`` are **top-level parameters, not keys inside
    the ``facets`` dict** — EIA rejects them as unknown facets otherwise. The full
    result is paged in (EIA's 5,000-row page cap is handled internally); a match above
    the row ceiling raises ``InvalidParameterError`` asking you to narrow it.
    """
    r = route.strip()
    if not r:
        raise InvalidParameterError("eia", "route must be non-empty")
    m = measure.strip()
    if not m:
        raise InvalidParameterError("eia", "measure must be non-empty")

    http = make_eia_client(api_key)
    base_params: dict[str, Any] = {
        "data[0]": m,
        "frequency": frequency,
        "start": start,
        "end": end,
        **_facet_params(facets),
    }
    rows, first = _paginate(http, f"{r}/data", base_params, op_name="eia_fetch")
    if not rows:
        raise EmptyDataError("eia", query_params={"route": r, "measure": m})

    title = str(first.get("description") or r)
    return _shape_observations(rows, key_column="route", key_value=r, title=title, measure=m)


@connector(output=EIA_SERIES_OUTPUT, tags=["macro", "energy", "us"], secrets=("api_key",), requires=("EIA_API_KEY",))
def eia_fetch_series(
    series_id: Annotated[str, "ns:eia"],
    start: str | None = None,
    end: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch an EIA series by its legacy APIv1 series id (e.g. ``PET.RWTC.D``).

    Uses the ``/v2/seriesid/{id}`` compatibility path, which reaches a fully
    specified series straight from its well-known id — the addressing scheme used
    across the EIA/FRED ecosystem and EIA's own data browsers. This path lives
    outside the route tree the catalog indexes, so it is the way to retrieve a
    famous series (WTI crude ``PET.RWTC.D``, Henry Hub ``NG.RNGWHHD.D``) without
    composing a route + facet query. Paged in full like ``eia_fetch``.
    """
    sid = series_id.strip()
    if not sid:
        raise InvalidParameterError("eia", "series_id must be non-empty")

    http = make_eia_client(api_key)
    base_params: dict[str, Any] = {"start": start, "end": end}
    rows, first = _paginate(http, f"seriesid/{sid}", base_params, op_name="eia_fetch_series")
    if not rows:
        raise EmptyDataError("eia", query_params={"series_id": sid})

    # A single series id resolves to one series, so its per-row series-description
    # (e.g. "Cushing, OK WTI Spot Price FOB") is a far better title than the
    # generic dataset-level `description`; fall back to that, then the id.
    series_desc = next(
        (str(row["series-description"]) for row in rows if str(row.get("series-description") or "").strip()),
        "",
    )
    title = series_desc or str(first.get("description") or sid)
    return _shape_observations(rows, key_column="series_id", key_value=sid, title=title)


@connector(output=EIA_FACETS_OUTPUT, tags=["macro", "energy", "us"], secrets=("api_key",), requires=("EIA_API_KEY",))
def eia_facets(
    route: Annotated[str, "ns:eia"],
    facet: str,
    api_key: str = "",
) -> pd.DataFrame:
    """List the valid values of one facet dimension of an EIA dataset.

    Given a dataset ``route`` and one of its facet ids (from the catalog's
    ``facets`` metadata), returns the ``{id, name}`` value vocabulary so a fetch
    can be narrowed to a specific series. This is the bridge that makes huge
    datasets usable — e.g. ``electricity/rto/region-data`` is ~18.7M rows, so an
    agent must narrow by ``respondent``/``fueltype`` facet values, which it
    discovers here rather than by a blind (ceiling-rejected) full fetch.
    """
    r = route.strip()
    f = facet.strip()
    if not r:
        raise InvalidParameterError("eia", "route must be non-empty")
    if not f:
        raise InvalidParameterError("eia", "facet must be non-empty")

    http = make_eia_client(api_key)
    resp = eia_get(http, f"{r}/facet/{f}", op_name="eia_facets")
    values = resp.get("facets") or []
    rows = [
        {
            "facet_value": str(v.get("id", "")),
            "name": str(v.get("name") or v.get("id", "")),
            "facet": f,
            "route": r,
        }
        for v in values
        if isinstance(v, dict) and v.get("id")
    ]
    if not rows:
        raise EmptyDataError("eia", query_params={"route": r, "facet": f})
    return pd.DataFrame(rows)
