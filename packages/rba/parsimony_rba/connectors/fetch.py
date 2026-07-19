"""RBA data fetch — resolves a ``table_id`` across all three publication formats.

A search hit carries a compound ``code`` = ``{table_id}#{series_id}``; the agent passes
the ``table_id`` portion to ``rba_fetch``. ``table_id`` takes one of three shapes, and
``rba_fetch`` routes each to the right publication format so **every** catalogued series
is fetchable (closing the prior CSV-only "catalog ⊋ connector" gap):

* ``f1-data`` / ``a3-es-balances-and-repo-agreements`` — a CSV stem (``rba_csv``).
* ``a03/Bond Purchase Program`` — a current XLSX workbook stem + sheet (``rba_xlsx``).
* ``b03hist`` or ``a03hist-2003-2008/<sheet>`` — a legacy xls-hist stem (``rba_xlsx_hist``).

Format is inferred from the ``table_id`` shape: a ``/`` separates a workbook stem from a
sheet; ``hist`` in the stem routes to the xls-hist host. Every catalogued xls-hist row's
stem contains ``hist``; the only ``rba_xlsx`` stem (``a03``) and all CSV stems do not.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError

from parsimony_rba import _http, parsing
from parsimony_rba.outputs import RBA_FETCH_OUTPUT


def _resolve_csv_url(session: _http.Session, table_id: str) -> str:
    """Scrape the RBA tables page and resolve *table_id* to a full CSV URL.

    Matches the table_id against the live CSV filenames on the page (exact first,
    then a ``<tid>-`` prefix fuzzy match) so an agent can pass ``f1`` for ``f1-data``
    and we never hard-code URL patterns that break when the RBA renames a file.
    """
    html = _http._curl_get(session, _http._TABLES_URL, op_name="tables_index")
    assert isinstance(html, str)
    matches = _http._CSV_LINK_PATTERN.findall(html)

    stem_to_path: dict[str, str] = {stem.lower(): path for path, stem in matches}
    tid = table_id.lower()

    if tid in stem_to_path:
        return f"{_http._BASE_URL}{stem_to_path[tid]}"
    for stem, path in stem_to_path.items():
        if stem.startswith(tid + "-") or stem == tid:
            return f"{_http._BASE_URL}{path}"

    available = sorted(stem_to_path.keys())[:20]
    raise InvalidParameterError(
        "rba",
        f"RBA table '{table_id}' not found. Available tables include: {', '.join(available)}...",
    )


def _get_bytes(session: _http.Session, url: str, *, op_name: str) -> bytes:
    data = _http._curl_get(session, url, op_name=op_name, binary=True)
    assert isinstance(data, bytes)
    return data


def _get_text(session: _http.Session, url: str, *, op_name: str) -> str:
    text = _http._curl_get(session, url, op_name=op_name)
    assert isinstance(text, str)
    return text


def _guard_parse(label: str, build: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    """Run a parse step, mapping any structural failure to ParseError (§6.4)."""
    try:
        return build()
    except Exception as exc:  # noqa: BLE001 — structural parse failure → ParseError
        raise ParseError("rba", f"could not parse RBA data for table {label!r}: {exc}") from exc


def _fetch_frame(session: _http.Session, table_id: str) -> pd.DataFrame:
    """Download + parse the requested table/sheet into the long fetch frame.

    Network/resolution failures (404/timeout, unknown table id) propagate as their
    typed errors; only the parse step is wrapped to ParseError. ``table_id`` shape
    selects the publication format.
    """
    raw = table_id.strip()

    if "/" in raw:
        # Workbook + explicit sheet: ``<stem>/<sheet>``. The sheet name is case- and
        # space-sensitive, so it is NOT lowercased.
        stem, sheet = raw.split("/", 1)
        stem = stem.strip().lower()
        sheet = sheet.strip()
        if "hist" in stem:
            url = f"{_http._BASE_URL}/statistics/tables/xls-hist/{stem}.xls"
            data = _get_bytes(session, url, op_name="xls_hist_fetch")
            return _guard_parse(raw, lambda: parsing._melt_sheet_rows(parsing._xls_full_sheet_rows(data, sheet), raw))
        url = f"{_http._BASE_URL}/statistics/tables/xls/{stem}.xlsx"
        data = _get_bytes(session, url, op_name="xlsx_fetch")
        return _guard_parse(raw, lambda: parsing._melt_sheet_rows(parsing._xlsx_full_sheet_rows(data, sheet), raw))

    stem = raw.lower()
    if stem.endswith(".csv"):
        stem = stem[:-4]
    if not stem:
        raise InvalidParameterError("rba", "table_id must be non-empty")

    if "hist" in stem:
        # A bare xls-hist stem is a single-data-sheet workbook; melt its data sheet(s).
        url = f"{_http._BASE_URL}/statistics/tables/xls-hist/{stem}.xls"
        data = _get_bytes(session, url, op_name="xls_hist_fetch")

        def _melt_all() -> pd.DataFrame:
            frames = [
                parsing._melt_sheet_rows(parsing._xls_full_sheet_rows(data, sheet), stem)
                for sheet in parsing._xls_data_sheet_names(data)
            ]
            frames = [f for f in frames if not f.empty]
            return pd.concat(frames, ignore_index=True) if frames else parsing._melt_sheet_rows([], stem)

        return _guard_parse(stem, _melt_all)

    url = _resolve_csv_url(session, stem)
    text = _get_text(session, url, op_name="csv")
    return _guard_parse(stem, lambda: parsing._parse_rba_csv(text, stem))


@connector(output=RBA_FETCH_OUTPUT, tags=["macro", "au"])
def rba_fetch(table_id: Annotated[str, Namespace("rba")]) -> pd.DataFrame:
    """Fetch an RBA statistical table/series by table ID.

    Resolves the table_id across RBA's three publication formats — CSV, current
    XLSX-exclusive sheet, and legacy xls-hist workbook — and returns the series as
    long-format ``date``/``value`` rows. The RBA site is Akamai-protected, so the
    fetch uses curl_cffi (browser impersonation); plain httpx is blocked at the TLS
    layer.
    """
    if not table_id.strip():
        raise InvalidParameterError("rba", "table_id must be non-empty")

    with _http._make_session() as session:
        df = _fetch_frame(session, table_id)

    if df.empty:
        raise EmptyDataError(
            "rba",
            message=f"No data returned for table: {table_id}",
            query_params={"table_id": table_id},
        )

    df["date"] = pd.to_datetime(df["date"])
    return df
