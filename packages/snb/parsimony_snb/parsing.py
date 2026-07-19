"""Pure parsing for SNB: sitemap → cube refs, cube CSV → frame, dimensions → series rows.

No I/O here — every function takes already-fetched text/JSON so it is unit-testable
offline. The catalog row shape produced here matches ``SNB_ENUMERATE_OUTPUT``.
"""

from __future__ import annotations

import io
import re
from itertools import product
from typing import Any

import pandas as pd
from parsimony.errors import ParseError

PROVIDER = "snb"

#: Cap on series rows emitted per publication cube. SNB exposes mega-cubes whose
#: dimension cartesian product exceeds this (e.g. ``frsekfutsek`` at 5,040); the
#: leaves are mostly redundant currency × counterpart × maturity crossings that
#: drown semantic signal in the embedder. Above the cap we collapse to one
#: cube-level row so the cube stays discoverable and the series stay fetchable
#: via ``dim_sel`` (the cardinality discipline). Warehouse cubes are always
#: catalogued at cube level for the same reason.
_MAX_SERIES_PER_CUBE = 100

# Sitemap <loc> shapes (parsed from the EN URLs; de/fr are duplicates).
_PUB_CUBE_RE = re.compile(r"<loc>https://data\.snb\.ch/en/topics/([^/]+)/cube/([^<]+)</loc>")
_WH_CUBE_RE = re.compile(r"<loc>https://data\.snb\.ch/en/warehouse/([^/]+)/cube/([^<]+)</loc>")

#: Publication topic code → human category (the sitemap path segment). The
#: authoritative category is ``getCubeInfo.publishingTitle``; this is the fallback.
_TOPIC_LABELS: dict[str, str] = {
    "snb": "Swiss National Bank",
    "banken": "Banks",
    "ziredev": "Interest rates, yields and foreign exchange",
    "finma": "Capital market and payment transactions",
    "uvo": "Swiss economic affairs",
    "aube": "International economic affairs",
    "cross": "Cross-thematic",
}

#: Warehouse group code → human category fallback.
_GROUP_LABELS: dict[str, str] = {
    "BSTA": "Banking statistics (warehouse)",
    "ZAST": "International / interest-rate statistics (warehouse)",
    "ZAHL": "Payment transactions (warehouse)",
    "DDUM": "Derivatives and FX turnover (warehouse)",
    "KRED": "Credit statistics (warehouse)",
    "SNB1A": "SNB balance sheet (warehouse)",
    "WKI": "Exchange rate indices (warehouse)",
}

_FREQ_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("dai", "Daily"),  # "Daily" / "daily" (note: "day" is not a substring of "daily")
    ("week", "Weekly"),
    ("month", "Monthly"),  # also matches "End of month"
    ("quart", "Quarterly"),  # "Quarterly" / "Quarter" / "End of quarter"
    ("year", "Annual"),
    ("annual", "Annual"),
)


# ---------------------------------------------------------------------------
# Sitemap → cube references
# ---------------------------------------------------------------------------


def parse_sitemap(xml_text: str) -> list[tuple[str, str, str]]:
    """Parse the SNB sitemap into ``[(cube_id, kind, topic_or_group)]``.

    ``kind`` is ``"publication"`` (bare cube id, under a topic) or ``"warehouse"``
    (SDMX ``@``/``.`` id, under a group). Deduplicated by cube id, first occurrence
    wins (a handful of publication cubes are linked under two topics — the topic is
    only a category hint, so the first is fine).
    """
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for topic, cube_id in _PUB_CUBE_RE.findall(xml_text):
        if cube_id not in seen:
            seen.add(cube_id)
            out.append((cube_id, "publication", topic))
    for group, cube_id in _WH_CUBE_RE.findall(xml_text):
        if cube_id not in seen:
            seen.add(cube_id)
            out.append((cube_id, "warehouse", group))
    return out


# ---------------------------------------------------------------------------
# Cube CSV → DataFrame
# ---------------------------------------------------------------------------


def parse_snb_csv(text: str, cube_id: str) -> pd.DataFrame:
    """Parse an SNB cube CSV download, skipping its metadata preamble.

    Long-format: a few preamble lines (``"CubeId";"<id>"`` / ``"PublishingDate"``),
    a blank line, then ``Date;<dim cols...>;Value`` and the data rows. The first
    column is the observation date, the trailing ``Value`` is the numeric measure,
    intermediate columns are string dimension codes.

    Coerces **only** the ``Value`` column to numeric (dimension codes stay strings —
    the eia blanket-coerce anti-pattern). Raises :class:`ParseError` when the 200
    body is not a parseable cube CSV (JSON error envelope / HTML page); returns an
    empty frame for a genuinely empty body (the caller maps that to EmptyData).
    """
    if text.startswith("﻿"):
        text = text[1:]

    stripped = text.strip()
    if not stripped:
        return pd.DataFrame()

    sep = ";" if ";" in stripped else ","
    lines = stripped.split("\n")

    # The header is the first line with 2+ separators (preamble lines carry one).
    header_idx: int | None = next((i for i, line in enumerate(lines) if line.count(sep) >= 2), None)
    if header_idx is None:
        raise ParseError(PROVIDER, f"cube {cube_id!r} returned a 200 body that is not a parseable cube CSV")

    data_text = "\n".join(lines[header_idx:])
    try:
        df = pd.read_csv(io.StringIO(data_text), sep=sep, dtype=str)
    except Exception as exc:  # noqa: BLE001 — surface any pandas parse failure as ParseError
        raise ParseError(PROVIDER, f"failed to parse SNB CSV for cube {cube_id!r}: {exc}") from exc

    if df.empty:
        return df

    df = df.rename(columns={df.columns[0]: "date"})
    value_col = df.columns[-1]
    if str(value_col).strip().lower() == "value":
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Dimensions tree → series rows
# ---------------------------------------------------------------------------


def is_measure_series(item: dict[str, Any]) -> bool:
    """Whether ``item`` is an addressable leaf (has an ``id``, no nested
    ``dimensionItems``) rather than a grouping node."""
    if not isinstance(item, dict) or not item.get("id"):
        return False
    children = item.get("dimensionItems")
    return not (isinstance(children, list) and children)


def collect_dimension_leaves(
    items: list[dict[str, Any]], parent_labels: tuple[str, ...] = ()
) -> list[tuple[str, tuple[str, ...]]]:
    """Walk a dimension's tree → ``(leaf_id, label_path)`` pairs, accumulating
    grouping-ancestor names into ``label_path``."""
    out: list[tuple[str, tuple[str, ...]]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("id") or ""
        if is_measure_series(item):
            out.append((str(item["id"]), parent_labels + (name,)))
            continue
        children = item.get("dimensionItems")
        if isinstance(children, list) and children:
            out.extend(collect_dimension_leaves(children, parent_labels + (name,)))
    return out


def normalize_frequency(spec: Any) -> str:
    """Map a ``getCubeInfo.frequencySpecification`` (freeform, e.g. "End of month")
    to a clean bucket, else ``"Unknown"``."""
    if not spec or not isinstance(spec, str):
        return "Unknown"
    low = spec.lower()
    for kw, label in _FREQ_KEYWORDS:
        if kw in low:
            return label
    return "Unknown"


def topic_label(kind: str, topic_or_group: str) -> str:
    """Fallback human category from the sitemap topic/group code."""
    if kind == "warehouse":
        return _GROUP_LABELS.get(topic_or_group, f"{topic_or_group} (warehouse)")
    return _TOPIC_LABELS.get(topic_or_group, topic_or_group)


def synthesize_title(cube_id: str, kind: str, topic_or_group: str) -> str:
    """A searchable title when ``getCubeInfo`` is unavailable — never an empty row."""
    return f"{cube_id} — {topic_label(kind, topic_or_group)}"


def _base_row(
    *,
    code: str,
    title: str,
    description: str,
    source: str,
    cube_id: str,
    series_key: str,
    dimension_path: str,
    category: str,
    frequency: str,
    unit: str,
) -> dict[str, str]:
    return {
        "code": code,
        "title": title,
        "description": description,
        "source": source,
        "cube_id": cube_id,
        "series_key": series_key,
        "dimension_path": dimension_path,
        "category": category,
        "frequency": frequency,
        "unit": unit,
    }


def cube_level_row(
    cube_id: str, *, source: str, title: str, category: str, frequency: str, unit: str
) -> dict[str, str]:
    """One coarse ``{cube_id}#`` row — for warehouse cubes, mega-cubes, and cubes
    whose dimensions are missing/unknown. The series stay fetchable via ``dim_sel``."""
    desc = f"{category}. {title}." if category and category != title else title
    return _base_row(
        code=f"{cube_id}#",
        title=title,
        description=desc,
        source=source,
        cube_id=cube_id,
        series_key="",
        dimension_path="",
        category=category,
        frequency=frequency,
        unit=unit,
    )


def series_from_dimensions(
    cube_id: str,
    *,
    cube_title: str,
    dimensions_payload: dict[str, Any] | None,
    source: str,
    category: str,
    frequency: str,
    unit: str,
) -> list[dict[str, str]]:
    """Cartesian-product a publication cube's dimension leaves into series rows.

    Code is ``{cube_id}#{series_key}`` where ``series_key`` joins the chosen leaf
    id per dimension with ``.`` (``rendoblim#10J``, ``devkum#M0.USD1``). A
    missing/empty dimensions payload, or a product over :data:`_MAX_SERIES_PER_CUBE`,
    collapses to a single cube-level row.
    """

    def _collapsed() -> list[dict[str, str]]:
        return [
            cube_level_row(cube_id, source=source, title=cube_title, category=category, frequency=frequency, unit=unit)
        ]

    dims = (dimensions_payload or {}).get("dimensions") or []
    per_dim_leaves: list[list[tuple[str, tuple[str, ...]]]] = []
    for dim in dims:
        if not isinstance(dim, dict):
            continue
        leaves = collect_dimension_leaves(dim.get("dimensionItems") or [])
        if leaves:
            per_dim_leaves.append(leaves)

    if not per_dim_leaves:
        return _collapsed()

    total = 1
    for leaves in per_dim_leaves:
        total *= len(leaves)
    if total > _MAX_SERIES_PER_CUBE:
        return _collapsed()

    rows: list[dict[str, str]] = []
    for combo in product(*per_dim_leaves):
        leaf_ids = [leaf_id for leaf_id, _ in combo]
        label_segments = [labels[-1] for _, labels in combo if labels]
        full_paths = [" / ".join(labels) for _, labels in combo if labels]
        series_key = ".".join(leaf_ids)
        dimension_path = " | ".join(full_paths)
        leaf_label = " / ".join(label_segments) if label_segments else series_key
        title = f"{leaf_label} — {cube_title}"
        description = f"{cube_title}. {dimension_path}." if dimension_path else cube_title
        rows.append(
            _base_row(
                code=f"{cube_id}#{series_key}",
                title=title,
                description=description,
                source=source,
                cube_id=cube_id,
                series_key=series_key,
                dimension_path=dimension_path,
                category=category,
                frequency=frequency,
                unit=unit,
            )
        )
    return rows
