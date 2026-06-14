"""Output schemas for the RBA connectors."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

RBA_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # Compound code ``{table_id}#{series_id}`` so every series gets a unique
        # catalog entry. RBA reuses some series IDs across closely-related tables
        # (e.g. ``b13-1-2-africa-and-middle-east`` vs ``b13-2-1-africa-and-middle-east``
        # share ~225 ids each); a bare ``series_id`` KEY would silently dedup ~5% of
        # entries. Mirrors Treasury's ``{endpoint}#{field}`` precedent. Agents split
        # on ``#`` to recover the fetchable ``table_id`` and the row's ``series_key``.
        Column(name="code", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` is the CSV/sheet header's own per-series descriptive text —
        # the most useful semantic signal for retrieval.
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` tells the agent which publication format a hit came from
        # (``rba_csv`` / ``rba_xlsx`` / ``rba_xlsx_hist``). All three are now fetchable
        # by ``rba_fetch`` (it resolves the ``table_id`` across the three formats).
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="table_id", role=ColumnRole.METADATA),
        Column(name="series_id", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
    ]
)

RBA_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="table_id", role=ColumnRole.KEY, namespace="rba"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
        Column(name="series_key", role=ColumnRole.DATA),
    ]
)

#: The exact column order an ``@enumerator`` must return (it enforces an exact match
#: and drops unmapped columns). Mirrors ``RBA_ENUMERATE_OUTPUT`` column order.
_ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "source",
    "table_id",
    "series_id",
    "category",
    "frequency",
    "unit",
)
