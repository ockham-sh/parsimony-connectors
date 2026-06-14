"""Output schemas for the US Treasury connectors."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

# Compound code ``{endpoint}#{field}`` so every addressable Fiscal Data measure has
# a unique catalog entry; agents split on ``#`` to recover the fetchable endpoint and
# the column to read off the row. ODM rate-feed rows use ``home/{feed}#{column}`` (the
# ``home/`` prefix + the ``source`` column route a search hit to the right fetch verb).
TREASURY_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` is the field's own descriptive text (Fiscal Data's ``definition``)
        # — the best retrieval signal. Named ``description`` so ``discovery_indexes`` indexes
        # it (the policy indexes code/title/description); a column named ``definition`` would
        # never be searched.
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` tells the agent which fetch connector to call: ``fiscal_data`` →
        # ``treasury_fetch``, ``treasury_rates`` → ``treasury_rates_fetch``.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="endpoint", role=ColumnRole.METADATA),
        Column(name="field", role=ColumnRole.METADATA),
        Column(name="data_type", role=ColumnRole.METADATA),
        Column(name="dataset", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="earliest_date", role=ColumnRole.METADATA),
        Column(name="latest_date", role=ColumnRole.METADATA),
    ]
)

#: The exact column order an ``@enumerator`` must return (it enforces an exact match
#: and drops unmapped columns). Mirrors ``TREASURY_ENUMERATE_OUTPUT`` order.
_ENUMERATE_COLUMNS: tuple[str, ...] = tuple(c.name for c in TREASURY_ENUMERATE_OUTPUT.columns)

# Treasury Fiscal Data returns tabular datasets — the output is a DataFrame whose
# columns depend on the endpoint. A minimal schema with just the identity key; the
# actual data columns vary per endpoint and fold in as DATA.
TREASURY_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="endpoint", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="record_date", dtype="datetime", role=ColumnRole.DATA),
    ]
)

# ODM rate feeds return one row per business day; the rate columns vary per feed
# (``BC_10YEAR`` for the par curve, ``ROUND_B1_YIELD_4WK_2`` for bills). The schema
# names only the columns we always materialise; the feed-specific rate columns fold in.
TREASURY_RATES_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="feed", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="record_date", dtype="datetime", role=ColumnRole.DATA),
    ]
)

__all__ = [
    "TREASURY_ENUMERATE_OUTPUT",
    "TREASURY_FETCH_OUTPUT",
    "TREASURY_RATES_FETCH_OUTPUT",
    "_ENUMERATE_COLUMNS",
]
