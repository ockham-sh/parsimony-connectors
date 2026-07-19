"""BdE catalog CSV parsing helpers."""

from __future__ import annotations

import csv
import io
import logging
import unicodedata
import zipfile

from parsimony_bde._http import CSV_HEADERS, FREQ_MAP_RAW, PB_CATEGORY

logger = logging.getLogger(__name__)


def _fold(text: str) -> str:
    """Upper-case and strip accents so Spanish row labels match regardless of
    encoding round-trips (``DESCRIPCIÓN`` == ``DESCRIPCION``)."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip().upper()


# Row labels (accent-folded) in the transposed ``pb.zip`` value files.
_PB_NOMBRE = "NOMBRE DE LA SERIE"
_PB_ALIAS = "ALIAS DE LA SERIE"
_PB_DESC = "DESCRIPCION DE LA SERIE"
_PB_UNIT = "DESCRIPCION DE LAS UNIDADES"
_PB_FREQ = "FRECUENCIA"
_PB_SEQ = "NUMERO SECUENCIAL"
_PB_META_LABELS = frozenset({_PB_NOMBRE, _PB_ALIAS, _PB_DESC, _PB_UNIT, _PB_FREQ, _PB_SEQ})


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


def _parse_pb_member(text: str) -> list[dict[str, str]]:
    """Parse one transposed ``pb_*.csv`` value file from ``pb.zip``.

    The Bank Lending Survey files are laid out column-per-series: row 0 holds the
    real fetchable ``serie`` codes (``DPB…``), row 2 their ``PB_x_y.z`` aliases,
    plus description / units / frequency rows, then one row per observation date.
    We read the metadata rows (the values are fetched live by ``bde_fetch``) and
    derive ``start_date`` / ``end_date`` / ``n_obs`` per column from the date
    rows so the catalog entry matches the richness of the CSV chapters.
    """
    reader = csv.reader(io.StringIO(text))
    by_label: dict[str, list[str]] = {}
    date_rows: list[tuple[str, list[str]]] = []
    for raw_row in reader:
        if not raw_row:
            continue
        label = _fold(raw_row[0])
        cells = [c.strip() for c in raw_row[1:]]
        if label in _PB_META_LABELS:
            by_label[label] = cells
        elif label:
            date_rows.append((raw_row[0].strip(), cells))

    names = by_label.get(_PB_NOMBRE, [])
    if not names:
        return []
    aliases = by_label.get(_PB_ALIAS, [])
    descs = by_label.get(_PB_DESC, [])
    units = by_label.get(_PB_UNIT, [])
    freqs = by_label.get(_PB_FREQ, [])

    rows: list[dict[str, str]] = []
    for j, raw_key in enumerate(names):
        key = raw_key.strip()
        # Real BdE series codes never contain whitespace; a value with a space is
        # a stray header label from a non-transposed member and is skipped.
        if not key or " " in key:
            continue
        # Span/count from the date rows where this column carries a value.
        present = [label for label, cells in date_rows if j < len(cells) and cells[j] != ""]
        desc = descs[j].strip() if j < len(descs) else ""
        freq_raw = freqs[j].strip().upper() if j < len(freqs) else ""
        rows.append(
            {
                "key": key,
                "title": desc or key,
                "description": desc,
                "source": "bde_biest",
                "alias": aliases[j].strip() if j < len(aliases) else "",
                "dataset": "",
                "category": PB_CATEGORY,
                "frequency": FREQ_MAP_RAW.get(freq_raw, freq_raw.title() if freq_raw else ""),
                "unit": units[j].strip() if j < len(units) else "",
                "decimals": "",
                "start_date": present[0] if present else "",
                "end_date": present[-1] if present else "",
                "n_obs": str(len(present)) if present else "",
                "source_org": "Banco de España",
            }
        )
    return rows


def parse_pb_zip(zip_bytes: bytes) -> list[dict[str, str]]:
    """Parse every member of the Bank Lending Survey ``pb.zip`` into enumerator
    rows keyed by the real fetchable ``DPB…`` series code.

    Best-effort, mirroring the CSV crawl: a corrupt member is logged and skipped
    so a partial survey still enumerates. De-dups within the archive by key
    (a series can recur across family files)."""
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            for member in archive.namelist():
                basename = member.rsplit("/", 1)[-1].lower()
                # The archive bundles the standard ``catalogo_pb.csv``
                # (un-fetchable alias catalog) next to the transposed
                # ``pb_*.csv`` value files. Only the value files carry the real
                # fetchable codes.
                if not (basename.startswith("pb_") and basename.endswith(".csv")):
                    continue
                try:
                    raw = archive.read(member)
                except (KeyError, zipfile.BadZipFile):
                    logger.warning("BdE pb.zip member %r unreadable; skipping", member)
                    continue
                try:
                    text = raw.decode("cp1252")
                except UnicodeDecodeError:
                    text = raw.decode("latin-1", errors="replace")
                for row in _parse_pb_member(text):
                    if row["key"] in seen:
                        continue
                    seen.add(row["key"])
                    rows.append(row)
    except zipfile.BadZipFile:
        logger.warning("BdE pb.zip is not a valid archive; skipping Bank Lending Survey")
        return []
    return rows
