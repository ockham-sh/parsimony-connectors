"""Parse the BLS bulk flat-file site (``download.bls.gov/pub/time.series``).

The authoritative per-survey universe lives in ``<survey>/<survey>.series`` — a
tab-separated file with one row per series carrying the dimension codes, a
(usually present) ``series_title``, and the active date range. Dimension code
tables (``<survey>/<survey>.<dim>``) map each dimension code to a human label.

The network functions (:func:`list_survey_dirs`, :func:`fetch_series_rows`,
:func:`fetch_dimension_tables`) are module-level seams: offline tests monkeypatch
them to inject canned flat-file data without touching the network. The pure
parsers below them have no I/O.
"""

from __future__ import annotations

import re
from typing import Any

from curl_cffi.requests import Session
from parsimony.errors import InvalidParameterError, ParseError

from parsimony_bls._http import DOWNLOAD_BASE, download_text
from parsimony_bls.surveys import normalize_survey

# IIS-style directory listing rows: "<date> <time> <AM|PM>  <dir>|<size> <A HREF=...>name</A>"
_LISTING_RE = re.compile(
    r'(?:&lt;dir&gt;|\d[\d,]*)\s+<A HREF="[^"]+">([^<]+)</A>',
)
_LISTING_SIZE_RE = re.compile(
    r'(&lt;dir&gt;|\d[\d,]*)\s+<A HREF="[^"]+">([^<]+)</A>',
)

#: Series-file columns that describe the series itself, not a queryable dimension.
STRUCTURAL_COLUMNS: frozenset[str] = frozenset(
    {
        "series_id",
        "series_title",
        "footnote_codes",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
        "base_period",
        "base_date",
        "base_code",
        "benchmark_year",
        "base_year",
    }
)

#: Refuse to index a ``.series`` file larger than this (the GB-scale microdata
#: surveys). Headline surveys top out ~16 MB, so this is generous for them.
MAX_SERIES_FILE_BYTES = 80_000_000

#: Dirs under ``time.series/`` that are not surveys with a ``.series`` file.
_NON_SURVEY_DIRS: frozenset[str] = frozenset({"compressed", "sdmx", "esbr", "yy"})


# --- pure parsers ----------------------------------------------------------


def parse_listing(html: str) -> list[tuple[str, int]]:
    """Return ``[(name, size_bytes)]`` from a download.bls.gov directory listing.

    Directories report size ``-1``. The link *text* (not the href) is the name,
    so dir names carry no trailing slash (e.g. ``"cu"``, not ``"cu/"``).
    """
    out: list[tuple[str, int]] = []
    for size_raw, name in _LISTING_SIZE_RE.findall(html):
        size = -1 if size_raw == "&lt;dir&gt;" else int(size_raw.replace(",", ""))
        out.append((name, size))
    return out


def parse_tsv(text: str) -> tuple[list[str], list[dict[str, str]]]:
    """Parse a BLS tab-separated flat file into (columns, rows of cleaned cells)."""
    lines = text.splitlines()
    if not lines:
        return [], []
    columns = [c.strip() for c in lines[0].split("\t")]
    rows: list[dict[str, str]] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        cells = line.split("\t")
        row = {col: (cells[i].strip() if i < len(cells) else "") for i, col in enumerate(columns)}
        rows.append(row)
    return columns, rows


def dimension_columns(columns: list[str]) -> list[str]:
    """The series columns that are queryable dimensions (codes), in file order."""
    return [c for c in columns if c and c not in STRUCTURAL_COLUMNS]


def _label_column(columns: list[str]) -> str | None:
    """Pick the human-label column of a dimension mapping table."""
    for suffix in ("_text", "_name", "_title", "_value", "_abbreviation", "_abbr"):
        for col in columns:
            if col.endswith(suffix):
                return col
    # Fall back to the second column (first is the code).
    return columns[1] if len(columns) >= 2 else None


def _table_suffix_candidates(col: str) -> list[str]:
    """Mapping-table suffixes to try for a dimension column.

    ``area_code`` → ``area``; ``data_type_code`` → ``data_type``/``datatype``;
    ``seasonal`` → ``seasonal``. First existing file wins at resolution time.
    """
    base = col.removesuffix("_code")
    cands = [base, base.replace("_", ""), col]
    seen: list[str] = []
    for c in cands:
        if c and c not in seen:
            seen.append(c)
    return seen


def build_label_map(columns: list[str], rows: list[dict[str, str]]) -> dict[str, str]:
    """Turn a parsed mapping table into ``{code: label}``."""
    if not columns:
        return {}
    code_col = columns[0]
    label_col = _label_column(columns)
    if label_col is None:
        return {}
    out: dict[str, str] = {}
    for row in rows:
        code = row.get(code_col, "").strip()
        label = row.get(label_col, "").strip()
        if code:
            out[code] = label or code
    return out


def resolve_label(tables: dict[str, dict[str, str]], col: str, code: str) -> str:
    """Best-effort dimension-code → label using the survey's mapping tables.

    Falls back to the raw code when no table/label is found, so structured search
    degrades gracefully rather than dropping the value.
    """
    if not code:
        return ""
    for suffix in _table_suffix_candidates(col):
        table = tables.get(suffix)
        if table and code in table:
            return table[code]
    return code


# --- network seams ---------------------------------------------------------


def list_survey_dirs(session: Session) -> list[str]:
    """Return the survey directory codes present on the download site (lower-case)."""
    html = download_text(session, f"{DOWNLOAD_BASE}/", op_name="root_listing")
    out: list[str] = []
    for name, size in parse_listing(html):
        if size != -1:
            continue
        code = name.rstrip("/").lower()
        if code and code not in _NON_SURVEY_DIRS and code.isalnum():
            out.append(code)
    return out


def _survey_listing(session: Session, survey: str) -> list[tuple[str, int]]:
    sv = normalize_survey(survey).lower()
    html = download_text(session, f"{DOWNLOAD_BASE}/{sv}/", op_name="survey_listing")
    return parse_listing(html)


def series_file_size(session: Session, survey: str) -> int:
    """Byte size of ``<survey>.series`` from the directory listing (-1 if absent)."""
    sv = normalize_survey(survey).lower()
    target = f"{sv}.series"
    for name, size in _survey_listing(session, survey):
        if name == target:
            return size
    return -1


def fetch_series_rows(
    session: Session,
    survey: str,
    *,
    max_bytes: int = MAX_SERIES_FILE_BYTES,
    max_rows: int = 0,
) -> tuple[list[str], list[dict[str, str]]]:
    """Fetch and parse one survey's ``.series`` file into (columns, rows).

    Guards on the listed file size first (the listing is a free oracle): refuses a
    file larger than *max_bytes* with actionable guidance, so an on-demand build
    never tries to slurp a GB-scale microdata survey. *max_rows* (0 = unlimited)
    caps the parsed rows as a second backstop.
    """
    sv = normalize_survey(survey).lower()
    size = series_file_size(session, survey)
    if size == -1:
        raise InvalidParameterError(
            "bls", f"survey {survey!r} has no .series file on download.bls.gov"
        )
    if size > max_bytes:
        raise InvalidParameterError(
            "bls",
            f"survey {survey!r} .series is {size:,} bytes (> {max_bytes:,}); too large to index. "
            "Use bls_surveys_search to read its dimension manifest, construct a series_id, "
            "and bls_fetch it directly.",
        )
    text = download_text(session, f"{DOWNLOAD_BASE}/{sv}/{sv}.series", op_name="series_file")
    columns, rows = parse_tsv(text)
    if not columns or columns[0] != "series_id":
        raise ParseError("bls", f"unexpected .series header for {survey!r}: {columns[:3]}")
    if max_rows and len(rows) > max_rows:
        rows = rows[:max_rows]
    return columns, rows


def fetch_dimension_tables(
    session: Session, survey: str, columns: list[str] | None = None
) -> dict[str, dict[str, str]]:
    """Load a survey's dimension mapping tables as ``{suffix: {code: label}}``.

    Discovers mapping files from the directory listing (every ``<sv>.<x>`` that is
    not the series/data/doc files). When *columns* is given, only the tables a
    dimension column could reference are fetched — fewer round-trips.
    """
    sv = normalize_survey(survey).lower()
    listing = _survey_listing(session, survey)
    wanted: set[str] | None = None
    if columns is not None:
        wanted = set()
        for col in dimension_columns(columns):
            wanted.update(_table_suffix_candidates(col))

    tables: dict[str, dict[str, str]] = {}
    for name, size in listing:
        if size == -1 or not name.startswith(f"{sv}."):
            continue
        suffix = name[len(sv) + 1 :]
        if not suffix or "." in suffix:  # skip data.* partitions and series
            continue
        if suffix in ("series", "txt", "contacts", "footnote", "period"):
            continue
        if wanted is not None and suffix not in wanted:
            continue
        text = download_text(
            session, f"{DOWNLOAD_BASE}/{sv}/{name}", op_name="dimension_table"
        )
        cols, rows = parse_tsv(text)
        label_map = build_label_map(cols, rows)
        if label_map:
            tables[suffix] = label_map
    return tables


def dimension_manifest(
    columns: list[str],
    rows: list[dict[str, str]],
    tables: dict[str, dict[str, str]],
    *,
    max_values: int = 12,
) -> list[dict[str, Any]]:
    """Compact ``[{id, values:[{code,label}…]}]`` manifest of a survey's dimensions.

    Mirrors the SDMX dimension manifest: up to *max_values* distinct (code, label)
    pairs per dimension, in first-seen order, for an agent to navigate / construct
    a series id.
    """
    out: list[dict[str, Any]] = []
    for col in dimension_columns(columns):
        seen: set[str] = set()
        values: list[dict[str, str]] = []
        for row in rows:
            code = row.get(col, "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            values.append({"code": code, "label": resolve_label(tables, col, code)})
            if len(values) >= max_values:
                break
        out.append({"id": col, "values": values})
    return out


__all__ = [
    "MAX_SERIES_FILE_BYTES",
    "STRUCTURAL_COLUMNS",
    "build_label_map",
    "dimension_columns",
    "dimension_manifest",
    "fetch_dimension_tables",
    "fetch_series_rows",
    "list_survey_dirs",
    "parse_listing",
    "parse_tsv",
    "resolve_label",
    "series_file_size",
]
