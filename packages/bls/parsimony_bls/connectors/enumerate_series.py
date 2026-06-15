"""``enumerate_bls_series`` — tier-2 feed: one row per series in ONE survey.

Reads the survey's authoritative ``.series`` flat file (curl_cffi, Akamai-walled
host) plus its dimension code tables, and emits one row per series with the
``series_id`` as code, a resolved title, and per-dimension ``<dim>_code`` /
``<dim>_label`` metadata for structured search. The output schema is **dynamic
per survey** (dimension columns differ), so this stays a plain ``@connector``
returning raw rows; the catalog builder re-projects with the per-survey schema.
"""

from __future__ import annotations

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError

from parsimony_bls._http import make_download_session
from parsimony_bls._titles import title_for_row
from parsimony_bls.flatfiles import (
    dimension_columns,
    fetch_dimension_tables,
    fetch_series_rows,
    resolve_label,
)
from parsimony_bls.outputs import BLS_SERIES_ENUM_OUTPUT
from parsimony_bls.surveys import normalize_survey


@connector(output=BLS_SERIES_ENUM_OUTPUT, tags=["macro", "us"])
def enumerate_bls_series(survey: str, max_rows: int = 0) -> pd.DataFrame:
    """Enumerate every series in one BLS survey from its ``.series`` flat file.

    ``max_rows`` (0 = unlimited) caps the parsed rows. A survey whose ``.series``
    file is too large to index (the GB-scale microdata surveys) raises
    ``InvalidParameterError`` with guidance to construct an id and ``bls_fetch``.
    """
    sv = normalize_survey(survey)
    with make_download_session() as session:
        columns, rows = fetch_series_rows(session, sv, max_rows=max_rows)
        tables = fetch_dimension_tables(session, sv, columns)

    if not rows:
        raise EmptyDataError("bls", query_params={"survey": sv})

    dim_cols = dimension_columns(columns)
    out: list[dict[str, object]] = []
    for row in rows:
        sid = row.get("series_id", "").strip()
        if not sid:
            continue
        rec: dict[str, object] = {
            "code": sid,
            "title": title_for_row(row, columns, tables),
            "survey": sv,
            "begin_year": row.get("begin_year", ""),
            "end_year": row.get("end_year", ""),
        }
        for col in dim_cols:
            dim = col.removesuffix("_code")
            code = row.get(col, "").strip()
            rec[f"{dim}_code"] = code
            rec[f"{dim}_label"] = resolve_label(tables, col, code)
        out.append(rec)

    if not out:
        raise EmptyDataError("bls", query_params={"survey": sv})

    return pd.DataFrame(out)


__all__ = ["enumerate_bls_series"]
