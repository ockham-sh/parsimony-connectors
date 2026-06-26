"""BdF connector output schemas."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

# The enumerator emits two entity kinds, distinguished by ``entity_type``:
#
# * ``series``  — KEY is the bare ``series_key`` (already globally unique, e.g.
#   ``EXR.M.USD.EUR.SP00.E`` — the dataflow prefix makes it unique, so no
#   compound code is needed).
# * ``dataset`` — a synthetic parent stub keyed ``dataset:{dataset_id}`` (45 of
#   these), mirroring BoJ's ``db:`` / BdP's ``dataset:`` so a consumer can split
#   entity kinds by KEY prefix alone (or by the ``entity_type`` column).
#
# ``title`` is the English short title (French / long / key fallback); the
# bilingual long titles plus the breadcrumb ``path`` and dataset context are
# folded into ``description`` so the discovery index (which also covers
# ``description``) gives recall in both languages.
BDF_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="description", role=ColumnRole.METADATA),
        Column(name="entity_type", role=ColumnRole.METADATA),  # "dataset" | "series"
        Column(name="dataset_id", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="ref_area", role=ColumnRole.METADATA),
        Column(name="source_agency", role=ColumnRole.METADATA),
        Column(name="path", role=ColumnRole.METADATA),  # EN breadcrumb hierarchy
        Column(name="first_time_period", role=ColumnRole.METADATA),
        Column(name="last_time_period", role=ColumnRole.METADATA),
    ]
)

BDF_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="bdf"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)

ENUMERATE_COLUMNS: tuple[str, ...] = tuple(c.name for c in BDF_ENUMERATE_OUTPUT.columns)
FETCH_COLUMNS: tuple[str, ...] = tuple(c.name for c in BDF_FETCH_OUTPUT.columns)

__all__ = [
    "BDF_ENUMERATE_OUTPUT",
    "BDF_FETCH_OUTPUT",
    "ENUMERATE_COLUMNS",
    "FETCH_COLUMNS",
]
