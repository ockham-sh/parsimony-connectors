"""Output schemas for the SNB connectors."""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

# Compound code ``{cube_id}#{series_key}`` so every addressable SNB series has a
# unique catalog entry; agents split on ``#`` to recover the fetchable cube_id and
# the dimension selection. Mirrors Treasury's ``{endpoint}#{field}`` and rba's
# ``{table_id}#{series_id}``. Warehouse cubes carry ``@``/``.`` in the cube_id but
# never ``#``, so the split is unambiguous.
SNB_ENUMERATE_OUTPUT = OutputSpec(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` carries cube + dimension-path prose so the embedder sees
        # the full human-readable series identity.
        Column(name="description", role=ColumnRole.METADATA),
        # ``source`` tells a dispatching agent which family a hit came from
        # (``snb_data_portal`` = publication cube, ``snb_warehouse`` = warehouse
        # cube). Both are fetchable by ``snb_fetch``.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="cube_id", role=ColumnRole.METADATA),
        Column(name="series_key", role=ColumnRole.METADATA),
        Column(name="dimension_path", role=ColumnRole.METADATA),
        # ``category`` is the cube's publishing context (getCubeInfo.publishingTitle,
        # else the sitemap topic/group label).
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
    ]
)

SNB_FETCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="cube_id", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", role=ColumnRole.DATA),
        # The cube's string dimension code columns (D0/D1/… or named warehouse
        # dimensions) and the numeric ``Value`` fold in as DATA automatically.
    ]
)

#: The exact column order an ``@enumerator`` must return (it enforces an exact
#: match and drops unmapped columns). Mirrors ``SNB_ENUMERATE_OUTPUT`` order.
_ENUMERATE_COLUMNS: tuple[str, ...] = (
    "code",
    "title",
    "description",
    "source",
    "cube_id",
    "series_key",
    "dimension_path",
    "category",
    "frequency",
    "unit",
)

__all__ = ["SNB_ENUMERATE_OUTPUT", "SNB_FETCH_OUTPUT", "_ENUMERATE_COLUMNS"]
