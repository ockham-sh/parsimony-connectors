"""BdP connector output schemas."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

# Enumerator / catalog feed. KEY shape:
#   * series  rows — ``"{domain_id}:{dataset_id}:{series_id}"``
#   * dataset rows — ``"dataset:{domain_id}:{dataset_id}"``
#   * domain  rows — ``"domain:{domain_id}"``
# The synthetic prefixes let downstream consumers split entity types by KEY
# alone (the ``entity_type`` column carries the same distinction explicitly).
BDP_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),  # "domain" | "dataset" | "series"
        Column(name="domain_id", role=ColumnRole.METADATA),
        Column(name="domain_name", role=ColumnRole.METADATA),
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(name="dataset_label", role=ColumnRole.METADATA),
        Column(name="title_pt", role=ColumnRole.METADATA),
        Column(name="short_label", role=ColumnRole.METADATA),
        Column(name="num_series", role=ColumnRole.METADATA),
        Column(name="last_update", role=ColumnRole.METADATA),
        Column(name="source", role=ColumnRole.METADATA),  # constant "bpstat"
    ]
)

# ``bdp_fetch`` is a general time-series fetch verb (KEY + TITLE + 2 DATA),
# decorated ``@connector`` — NOT ``@loader`` — so the human-readable series
# label may ride as a TITLE column. The KEY (``series_id``) maps back to the
# ``dataset_id`` parameter (``param_key``) so the namespace hint marks
# ``dataset_id`` as a ``bdp`` value for the search→fetch dispatch.
BDP_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="series_id", role=ColumnRole.KEY, namespace="bdp"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "entity_type",
    "domain_id",
    "domain_name",
    "dataset_id",
    "dataset_label",
    "title_pt",
    "short_label",
    "num_series",
    "last_update",
    "source",
)

FETCH_COLUMNS: tuple[str, ...] = ("series_id", "title", "date", "value")
