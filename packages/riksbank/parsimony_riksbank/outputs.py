"""Output schemas for the Riksbank connectors (enumerate + five fetch families).

The enumerator emits one row per addressable unit across all five products with a
single routable ``code`` KEY and a ``source`` METADATA column that records which family
a hit belongs to. Routing is by code shape: SWEA/SWESTR codes are bare ids; the other
three families carry a ``<family>/...`` prefix (see each family module).
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

RIKSBANK_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` is the long-form prose the catalog text index searches.
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` records the family: swea | swestr | monetary_policy | turnover | holdings.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="group", role=ColumnRole.METADATA),
        Column(name="provider", role=ColumnRole.METADATA),
        Column(name="observation_min", role=ColumnRole.METADATA),
        Column(name="observation_max", role=ColumnRole.METADATA),
        Column(name="series_closed", role=ColumnRole.METADATA),
    ]
)

ENUMERATE_COLUMNS: tuple[str, ...] = tuple(c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns)

# --- SWEA fetch ---
RIKSBANK_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

# --- SWESTR fetch (native trade metadata folds in as extra DATA columns) ---
RIKSBANK_SWESTR_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

# --- Monetary Policy fetch (policy_round + forecast_cutoff_date fold in) ---
RIKSBANK_MONETARY_POLICY_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

# --- Turnover fetch (asset/contract/counterparty facets fold in) ---
RIKSBANK_TURNOVER_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="market", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="period", role=ColumnRole.DATA),
        Column(name="amount", role=ColumnRole.DATA),
    ]
)

# --- Holdings fetch (security group / issuer / ISIN / maturity fold in) ---
RIKSBANK_HOLDINGS_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="dataset", role=ColumnRole.KEY, namespace="riksbank"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="balance_nominal_number", role=ColumnRole.DATA),
    ]
)

__all__ = [
    "RIKSBANK_ENUMERATE_OUTPUT",
    "ENUMERATE_COLUMNS",
    "RIKSBANK_FETCH_OUTPUT",
    "RIKSBANK_SWESTR_FETCH_OUTPUT",
    "RIKSBANK_MONETARY_POLICY_FETCH_OUTPUT",
    "RIKSBANK_TURNOVER_FETCH_OUTPUT",
    "RIKSBANK_HOLDINGS_FETCH_OUTPUT",
]
