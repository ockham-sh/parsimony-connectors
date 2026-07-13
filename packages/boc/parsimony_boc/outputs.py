"""Output schemas for the Bank of Canada (BoC) connectors."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

BOC_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="series_name", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        Column(name="value", role=ColumnRole.DATA),
    ]
)

BOC_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        # The KEY is either a series name (e.g. ``FXUSDCAD``) or a group entry
        # prefixed with ``group:`` (e.g. ``group:FX_RATES_DAILY``). Groups are
        # first-class addressable entities — ``boc_fetch`` accepts
        # ``series_name="group:NAME"`` and Valet's
        # ``/observations/group/{name}/json`` returns the full panel — so they
        # get their own catalog rows for discovery alongside the per-series rows.
        # The ``group:`` prefix is the exact string ``boc_fetch`` already
        # expects, so a search hit routes straight to a fetch.
        Column(name="series_name", role=ColumnRole.KEY, namespace="boc"),
        Column(name="title", role=ColumnRole.TITLE),
        # The upstream Valet ``description`` text. For group rows this carries
        # the group's description (often the only place units/frequency live,
        # e.g. "Month-end, Millions of dollars").
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` tells the agent which fetch connector to call. BoC has a
        # single Valet source today; the column is future-proofing for a
        # parallel source so dispatch is already wired (matches Treasury's
        # ``fiscal_data``/``treasury_rates`` split).
        Column(name="source", role=ColumnRole.METADATA),
        # ``entity_type`` is ``"series"`` for individual series rows and
        # ``"group"`` for group rows — lets agents filter or weight by
        # entity granularity.
        Column(name="entity_type", role=ColumnRole.METADATA),
        # ``group`` carries the upstream group ID the series belongs to. A
        # series can belong to several groups (rare); the first encountered
        # group wins. Empty string when a series is in no catalogued group.
        # For group rows this is the group's own ID.
        Column(name="group", role=ColumnRole.METADATA),
        Column(name="group_label", role=ColumnRole.METADATA),
    ]
)

#: The exact column order the ``@enumerator`` must return (enumerators drop
#: unmapped columns then require an exact match against the declared schema).
ENUMERATE_COLUMNS: tuple[str, ...] = tuple(c.name for c in BOC_ENUMERATE_OUTPUT.columns)

__all__ = ["BOC_ENUMERATE_OUTPUT", "BOC_FETCH_OUTPUT", "ENUMERATE_COLUMNS"]
