"""EIA catalog enumeration: walk the v2 route tree to one row per leaf dataset.

The EIA v2 API is a hierarchy of *route nodes*. A node either lists child
``routes`` (a category) or terminates as a *leaf dataset* carrying ``data`` (the
measures), ``facets`` (the dimensions), ``frequency`` and a ``startPeriod`` /
``endPeriod``. Walking the tree from ``/v2/`` and stopping at the leaves yields
the authoritative full universe of addressable datasets (archetype B fan-out;
~232 leaves at the time of writing, ~272 node fetches). Route-node child lists
are inline and never paginated, so the walk is complete with no list-pagination
trap.

Each leaf becomes one catalog row whose metadata is a *dimension manifest*: the
measures it accepts as ``data[0]=`` and the facet ids it accepts as ``facets[]``
filters, with the measure/facet vocabulary also folded into the indexed
``description`` so it is BM25-findable. The series within a dataset (the facet
cartesian product) are not catalogued — they are fetchable by id via
``eia_fetch`` facet filters, so discovery is at the dataset tier and access is
total (the BLS/SDMX two-tier split).

Best-effort: a node whose fetch fails is logged and skipped (its subtree is lost
for that run) so a transient blip yields a partial — not empty — catalog.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from parsimony.connector import enumerator
from parsimony.errors import ConnectorError
from parsimony.transport import HttpClient, check_status, pooled_client

from parsimony_eia._http import PROVIDER, make_eia_client
from parsimony_eia.outputs import EIA_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)

# Cap the embedded raw description so a verbose dataset blurb doesn't dominate the
# indexed text (mirrors parsimony_shared's embedder-friendly cap without adding a
# dependency for one constant).
DESCRIPTION_CHAR_CAP = 1500


def _get_node(
    client: HttpClient,
    route: str,
) -> dict[str, Any] | None:
    """Best-effort GET of one route node's metadata. ``None`` on any failure."""
    path = f"/{route}" if route else "/"
    try:
        resp = client.request("GET", path, op_name="eia_enumerate")
        check_status(resp, provider=PROVIDER, op_name="eia_enumerate")
        body = resp.json()
    except (ConnectorError, ValueError) as exc:
        logger.warning("EIA enumerate: node %r failed: %s", route or "<root>", exc)
        return None
    inner = body.get("response") if isinstance(body, dict) else None
    return inner if isinstance(inner, dict) else None


def _load_top_routes(client: HttpClient) -> list[str]:
    """Return the top-level route ids from ``/v2/``.

    This is the **bounding seam** for live tests: monkeypatch it to a 1–2 route
    slice so the tree walk fires a handful of requests instead of crawling all
    ~272 nodes. ``enumerate_eia`` calls it by bare name, so the patch takes.
    """
    root = _get_node(client, "")
    if root is None:
        return []
    return [str(c["id"]) for c in (root.get("routes") or []) if isinstance(c, dict) and c.get("id")]


def _walk(
    client: HttpClient,
    routes: list[str],
    leaves: list[dict[str, Any]],
) -> None:
    """Breadth-first serial walk: fetch each node in turn, recurse into children,
    collect leaf datasets (annotated with their route path) into ``leaves``."""
    next_level: list[str] = []
    for route in routes:
        node = _get_node(client, route)
        if node is None:
            continue
        child_routes = node.get("routes")
        if isinstance(child_routes, list) and child_routes:
            next_level.extend(f"{route}/{c['id']}" for c in child_routes if isinstance(c, dict) and c.get("id"))
        else:
            node["__route__"] = route
            leaves.append(node)
    if next_level:
        _walk(client, next_level, leaves)


def _measure_units(data: dict[str, Any], measures: list[str]) -> list[str]:
    seen: list[str] = []
    for m in measures:
        units = str((data.get(m) or {}).get("units") or "").strip() if isinstance(data.get(m), dict) else ""
        if units and units not in seen:
            seen.append(units)
    return seen


def _synthesize_description(
    *,
    name: str,
    category: str,
    raw_description: str,
    measures: list[str],
    units: list[str],
    facet_pairs: list[tuple[str, str]],
    frequencies: list[str],
    start: str,
    end: str,
) -> str:
    """Fold the dataset's query vocabulary into one indexed description string."""
    parts: list[str] = []
    if name:
        parts.append(name)
    parts.append(f"{category} energy data from the U.S. Energy Information Administration (EIA)")
    if raw_description:
        parts.append(raw_description[:DESCRIPTION_CHAR_CAP])
    if measures:
        m = ", ".join(measures)
        parts.append(f"Measures: {m}" + (f" ({', '.join(units)})" if units else ""))
    if facet_pairs:
        rendered = ", ".join(f"{fid} ({desc})" if desc else fid for fid, desc in facet_pairs)
        parts.append(f"Facets: {rendered}")
    if frequencies:
        parts.append(f"Frequencies: {', '.join(frequencies)}")
    if start or end:
        parts.append(f"Coverage {start or '?'}..{end or '?'}")
    return ". ".join(p for p in parts if p)


def _dataset_row(node: dict[str, Any]) -> dict[str, str]:
    """Build one catalog row (the dimension manifest) from a leaf dataset node."""
    route = str(node.get("__route__") or "")
    name = str(node.get("name") or route)
    raw_description = str(node.get("description") or "")
    category = route.split("/", 1)[0]

    data = node.get("data") if isinstance(node.get("data"), dict) else {}
    measures = [str(k) for k in (data or {})]
    units = _measure_units(data or {}, measures)

    facets_raw = node.get("facets") if isinstance(node.get("facets"), list) else []
    facet_pairs: list[tuple[str, str]] = []
    for f in facets_raw or []:
        if isinstance(f, dict) and f.get("id"):
            facet_pairs.append((str(f["id"]), str(f.get("description") or "").strip()))
    facet_ids = [fid for fid, _ in facet_pairs]

    freqs_raw = node.get("frequency") if isinstance(node.get("frequency"), list) else []
    frequencies = [str(f["id"]) for f in (freqs_raw or []) if isinstance(f, dict) and f.get("id")]

    start = str(node.get("startPeriod") or "")
    end = str(node.get("endPeriod") or "")

    return {
        "code": route,
        "title": name,
        "description": _synthesize_description(
            name=name,
            category=category,
            raw_description=raw_description,
            measures=measures,
            units=units,
            facet_pairs=facet_pairs,
            frequencies=frequencies,
            start=start,
            end=end,
        ),
        "category": category,
        "measures": ",".join(measures),
        "facets": ",".join(facet_ids),
        "frequencies": ",".join(frequencies),
        "default_frequency": str(node.get("defaultFrequency") or ""),
        "start": start,
        "end": end,
        "units": ",".join(units),
    }


@enumerator(output=EIA_ENUMERATE_OUTPUT, tags=["macro", "energy", "us"], secrets=("api_key",))
def enumerate_eia(api_key: str = "") -> pd.DataFrame:
    """Enumerate every EIA v2 leaf dataset by walking the route tree.

    Emits one row per addressable dataset (KEY=route path) carrying its measure
    and facet manifest, for catalog indexing. A missing key fast-fails with
    ``UnauthorizedError`` before any network call.
    """
    http = make_eia_client(api_key)
    leaves: list[dict[str, Any]] = []

    with pooled_client(http) as shared:
        top_routes = _load_top_routes(shared)
        if not top_routes:
            logger.warning("EIA enumerate: /v2/ returned no top-level routes")
            return pd.DataFrame(columns=list(ENUMERATE_COLUMNS))
        _walk(shared, top_routes, leaves)

    rows = [_dataset_row(leaf) for leaf in leaves]
    logger.info("EIA enumerate: %d leaf datasets across %d top-level routes", len(rows), len(top_routes))
    return pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS))
