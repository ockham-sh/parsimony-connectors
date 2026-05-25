"""BdE catalog CSV parsing helpers."""

from __future__ import annotations

import csv
import io
import logging

from parsimony_bde._http import CSV_HEADERS, FREQ_MAP_RAW

logger = logging.getLogger(__name__)


def split_title_path(raw: str) -> tuple[str, str]:
    """Split a "/"-separated BdE title-path into (dataset, leaf_title)."""
    if "/" not in raw:
        return "", raw.strip()
    parts = [p.strip() for p in raw.split("/") if p.strip()]
    if not parts:
        return "", raw.strip()
    if len(parts) == 1:
        return "", parts[0]
    if all(":" in p for p in parts):
        return " › ".join(parts), ""
    return " › ".join(parts[:-1]), parts[-1]


def parse_catalog_csv(text: str, *, category: str) -> list[dict[str, str]]:
    """Parse one ``catalogo_*.csv`` payload into enumerator rows."""
    reader = csv.reader(io.StringIO(text))
    rows: list[dict[str, str]] = []

    header_seen = False
    for raw_row in reader:
        if not raw_row:
            continue
        if not header_seen:
            header_seen = True
            continue
        if len(raw_row) < len(CSV_HEADERS):
            logger.debug(
                "skipping malformed BdE catalog row (got %d cols, expected %d)",
                len(raw_row),
                len(CSV_HEADERS),
            )
            continue

        record = dict(zip(CSV_HEADERS, raw_row, strict=False))
        serie = (record.get("serie") or "").strip()
        if not serie:
            continue

        title_raw = (record.get("title") or "").strip()
        dataset, leaf_title = split_title_path(title_raw)
        title = leaf_title or (record.get("description") or "").strip() or serie

        freq_raw = (record.get("frequency_raw") or "").strip().upper()
        frequency = FREQ_MAP_RAW.get(freq_raw, freq_raw.title() if freq_raw else "")

        rows.append(
            {
                "key": serie,
                "title": title,
                "description": (record.get("description") or "").strip(),
                "source": "bde_biest",
                "alias": (record.get("alias") or "").strip(),
                "dataset": dataset,
                "category": category,
                "frequency": frequency,
                "unit": (record.get("unit_desc") or record.get("unit_code") or "").strip(),
                "decimals": (record.get("decimals") or "").strip(),
                "start_date": (record.get("start_date") or "").strip(),
                "end_date": (record.get("end_date") or "").strip(),
                "n_obs": (record.get("n_obs") or "").strip(),
                "source_org": (record.get("source_org") or "").strip(),
            }
        )
    return rows
