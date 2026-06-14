"""Output schemas for the Bank of Japan connectors.

* ``BOJ_FETCH_OUTPUT`` — ``boj_fetch`` observations: KEY ``code`` (ns ``boj``) +
  TITLE ``title`` + DATA ``date``/``value``.
* ``BOJ_ENUMERATE_OUTPUT`` — the catalog feed: KEY ``code`` (ns ``boj``) + TITLE
  ``title`` + searchable METADATA. ``code`` is a bare series code for series rows
  or ``db:<code>`` for the synthetic per-database rows (mirrors BoC's ``group:``).
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

BOJ_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # KEY: bare series code (e.g. ``FXERD01``) for series rows, or
        # ``db:<code>`` for DB-level rows. The ``db:`` prefix lets agents and the
        # catalog-build splitter distinguish DB rows from series rows by KEY.
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        # TITLE: ``NAME_OF_TIME_SERIES`` for series rows; canonical DB title for DB rows.
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` carries searchable prose: breadcrumb + category + unit +
        # frequency + parent DB title (+ NOTES) for series; a summary for DB rows.
        Column(name="description", role=ColumnRole.METADATA),
        # METADATA columns (filtering / dispatch / UI hints):
        Column(name="db", role=ColumnRole.METADATA),
        Column(name="db_title", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),  # "series" | "db"
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="breadcrumb", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
        Column(name="last_update", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),  # constant "stat_search"
    ]
)

BOJ_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="boj"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

#: Declared enumerate columns, in order — the @enumerator contract requires an
#: exact column match against this on every call.
ENUMERATE_COLUMNS: tuple[str, ...] = tuple(c.name for c in BOJ_ENUMERATE_OUTPUT.columns)


__all__ = ["BOJ_ENUMERATE_OUTPUT", "BOJ_FETCH_OUTPUT", "ENUMERATE_COLUMNS"]
