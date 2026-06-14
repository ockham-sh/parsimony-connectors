"""SNB catalog enumerator — live sitemap discovery (archetype A).

The authoritative universe is the published XML **sitemap** (``/sitemap``), which
lists every cube URL: publication cubes (``/topics/{topic}/cube/{id}``) and
warehouse cubes (``/warehouse/{group}/cube/{sdmx_id}``). This replaces the old
frozen ``_KNOWN_CUBES`` registry (the guidebook's named cautionary case) with a
self-tracking enumeration — a new SNB cube appears in the sitemap automatically.

Per cube, a serial fan-out fetches:

* ``getCubeInfo`` (best-effort) → title / publishingTitle / unit / frequency;
* for **publication** cubes, ``/dimensions`` → one row per series (cartesian
  product, mega-cubes collapse to a cube-level row).

**Warehouse** cubes are catalogued at *cube* level (one row each): their cartesian
products are enormous and the leaves stay fetchable via ``dim_sel`` (the
cardinality discipline). Per-cube failures are skipped; only the sitemap fetch
failing is fatal.
"""

from __future__ import annotations

import logging

import pandas as pd
from parsimony.connector import enumerator
from parsimony.transport import pooled_client

from parsimony_snb import _http, parsing
from parsimony_snb.outputs import _ENUMERATE_COLUMNS, SNB_ENUMERATE_OUTPUT

logger = logging.getLogger(__name__)

_PUBLICATION_SOURCE = "snb_data_portal"
_WAREHOUSE_SOURCE = "snb_warehouse"


def _list_cubes() -> list[tuple[str, str, str]]:
    """The discovery seam: parse ``/sitemap`` → ``[(cube_id, kind, topic_or_group)]``.

    Read at call time so a live/offline test can monkeypatch it to a 2–3 cube slice
    and bound the per-cube fan-out (instead of crawling all ~1,149 cubes).
    """
    http = _http.client()
    text = _http.fetch_sitemap(http)
    return parsing.parse_sitemap(text)


def _probe_cube(
    client: _http.HttpClient,
    cube_id: str,
    kind: str,
    topic_or_group: str,
) -> list[dict[str, str]]:
    """Fetch one cube's metadata (+ dimensions for publication cubes) → catalog rows."""
    info = _http.get_cube_info(client, cube_id, lang="en")
    info = info or {}
    category = str(info.get("publishingTitle") or "").strip() or parsing.topic_label(kind, topic_or_group)
    title = str(info.get("title") or "").strip() or parsing.synthesize_title(cube_id, kind, topic_or_group)
    unit = str(info.get("unit") or "").strip()
    frequency = parsing.normalize_frequency(info.get("frequencySpecification"))

    if kind == "warehouse":
        return [
            parsing.cube_level_row(
                cube_id, source=_WAREHOUSE_SOURCE, title=title, category=category, frequency=frequency, unit=unit
            )
        ]

    dims = _http.get_dimensions(client, cube_id, lang="en")
    return parsing.series_from_dimensions(
        cube_id,
        cube_title=title,
        dimensions_payload=dims,
        source=_PUBLICATION_SOURCE,
        category=category,
        frequency=frequency,
        unit=unit,
    )


@enumerator(output=SNB_ENUMERATE_OUTPUT, tags=["macro", "ch"])
def enumerate_snb() -> pd.DataFrame:
    """Enumerate every SNB cube (publication + warehouse) as catalog rows.

    The universe is discovered live from the sitemap (the ``_list_cubes`` seam);
    publication cubes expand to per-series rows (compound ``cube_id#series_key``
    codes), warehouse cubes emit one cube-level row each. The metadata fan-out is
    serial over a single pooled client; per-cube failures are skipped.
    """
    cubes = _list_cubes()
    if not cubes:
        return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))

    base = _http.client()
    rows: list[dict[str, str]] = []
    with pooled_client(base) as shared:
        for cube_id, kind, group in cubes:
            cube_rows = _probe_cube(shared, cube_id, kind, group)
            rows.extend(cube_rows)

    if not rows:
        return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))
    return pd.DataFrame(rows, columns=list(_ENUMERATE_COLUMNS))
