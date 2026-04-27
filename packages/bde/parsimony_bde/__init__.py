"""Banco de España (BdE): fetch + catalog enumeration.

API docs: https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html
Series search: https://app.bde.es/bie_www/bie_wwwias/xml/Arranque.html (BIEST)
No authentication required.

The catalog enumerator pulls BdE's own published catalog CSVs — seven chapters
(``catalogo_{be,cf,ie,pb,si,tc,ti}``) covering ~20,450 rows (≈15,500 unique
series codes after de-duplication across overlapping chapters) spanning general
statistics, financial accounts of the Spanish economy, international economy,
bank lending surveys, financial indicators, exchange rates, and interest rates.
BdE has no queryable list endpoint and no SDMX feed of its own; the CSV
directory is the only discovery surface, and it carries the descriptive prose,
frequency, units, date ranges, and dataset grouping needed for high-recall
semantic search. An exhaustive probe of ``catalogo_{aa..zz}.csv`` confirms no
other 2-letter chapter resolves to HTTP 200.
"""

from __future__ import annotations

import contextlib
import csv
import io
import logging
from datetime import datetime
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport import map_http_error
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


_BASE_URL = "https://app.bde.es/bierest/resources/srdatosapp"

# BdE publishes its statistical catalog as seven CSV files, one per "chapter":
#
#   * BE — General Statistics (national accounts, prices, employment, …)
#   * CF — Financial Accounts of the Spanish Economy (CFEE, SEC2010 sector
#          balance sheets; ~4.7k rows, several hundred overlap with BE).
#   * IE — International Economy (world prices, commodity indices; ~93 rows,
#          mostly overlapping BE but retained for category-filtered search).
#   * PB — Bank Lending Survey
#   * SI — Financial Indicators (confidence indices, retail trade, …)
#   * TC — Exchange Rates
#   * TI — Interest Rates
#
# An exhaustive probe of ``catalogo_{aa..zz}.csv`` (676 combinations) on
# 2026-04-24 confirms these seven are the complete published set; every other
# 2-letter code 302s to a 404 page. Only the Spanish (``es``) variant resolves;
# the ``en`` URL 302s to a 404. The catalog itself contains both Spanish and
# English-translatable descriptions in its ``descripcion`` and ``titulo``
# columns; we surface them as-is and let downstream embedders handle the
# bilingual content.
_CATALOG_CSV_BASE_URL = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
_CATALOG_CHAPTERS: tuple[tuple[str, str], ...] = (
    ("be", "General Statistics"),
    ("cf", "Financial Accounts"),
    ("ie", "International Economy"),
    ("pb", "Bank Lending Survey"),
    ("si", "Financial Indicators"),
    ("tc", "Exchange Rates"),
    ("ti", "Interest Rates"),
)

# BdE catalog CSVs are encoded in CP1252 (Latin-1 superset). Lowercase column
# headers map onto our schema. Index keys match the ``Nombre de la serie``
# header (Spanish, with a trailing space the publisher kept since the 90s).
_CSV_ENCODING = "cp1252"
_CSV_HEADERS: tuple[str, ...] = (
    "serie",          # Internal API code — fed to /listaSeries.
    "seq",            # Numeric sequential id (unused).
    "alias",          # Public alias like "TI_1_1.1".
    "file",           # Source CSV file on bde.es.
    "description",    # Long descriptive prose (Spanish).
    "var_type",       # MEDIA / SUMA / FINAL aggregation kind.
    "unit_code",      # ISO/internal unit code (EUR, %, USD/EUR, …).
    "exponent",       # Power-of-ten scale on stored values.
    "decimals",       # Display precision.
    "unit_desc",      # Human-readable unit ("Millones de euros").
    "frequency_raw",  # MENSUAL / TRIMESTRAL / DIARIA / LABORABLE / ANUAL.
    "start_date",     # Spanish-format first observation ("MAR 1995").
    "end_date",       # Spanish-format last observation ("DIC 2025").
    "n_obs",          # Observation count.
    "title",          # "/"-separated taxonomic path.
    "source_org",     # Originating organisation (INE, BCE, BdE, …).
    "notes",          # Methodological remarks.
)

# BdE chapter codes use Spanish frequency labels; we normalise to English to
# match Treasury / FRED conventions and so an agent searching "monthly" hits
# Spanish series that were originally labelled "MENSUAL".
_FREQ_MAP_RAW = {
    "DIARIA": "Daily",
    "LABORABLE": "Business Daily",
    "SEMANAL": "Weekly",
    "QUINCENAL": "Bi-weekly",
    "MENSUAL": "Monthly",
    "TRIMESTRAL": "Quarterly",
    "SEMESTRAL": "Semi-annual",
    "ANUAL": "Annual",
    # The /listaSeries endpoint returns single-letter codes for the same
    # frequencies; we keep both maps in sync so bde_fetch can label rows.
}

# Frequency single-letter code (from /favoritas, /listaSeries) → English.
_FREQ_MAP = {
    "D": "Daily",
    "M": "Monthly",
    "Q": "Quarterly",
    "A": "Annual",
    "S": "Semi-annual",
    "W": "Weekly",
    "B": "Business Daily",
}

# Spanish ↔ English column mapping used by the fetch parser.
_COLUMN_MAP = {
    "serie": "key",
    "descripcion": "description",
    "descripcionCorta": "title",
    "codFrecuencia": "freq",
    "decimales": "decimals",
    "simbolo": "symbol",
    "fechaInicio": "start_date",
    "fechaFin": "end_date",
    "fechas": "date",
    "valores": "value",
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class BdeFetchParams(BaseModel):
    """Parameters for fetching Banco de España time series."""

    key: Annotated[str, "ns:bde"] = Field(
        ...,
        description="Comma-separated BdE series codes (e.g. D_1NBAF472)",
    )
    time_range: str | None = Field(
        default=None,
        description=("Time range: 30M, 60M, MAX, or a year (e.g. 2024). Default uses the full available range."),
    )
    lang: str = Field(default="en", description="Language: en or es")

    @field_validator("key")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("At least one series code required")
        return v

    @field_validator("time_range")
    @classmethod
    def _valid_range(cls, v: str | None) -> str | None:
        if v is None:
            return v
        v = v.strip()
        # BdE API rejects short range codes (3M, 12M); accept 30M, 60M, MAX, or year
        _VALID_RANGES = {"30M", "60M", "MAX"}
        if v.upper() in _VALID_RANGES or v.isdigit():
            return v
        raise ValueError(f"Invalid time_range '{v}'. Use 30M, 60M, MAX, or a year (e.g. 2024).")

    @field_validator("lang")
    @classmethod
    def _valid_lang(cls, v: str) -> str:
        if v not in ("en", "es"):
            raise ValueError("lang must be 'en' or 'es'")
        return v


class BdeEnumerateParams(BaseModel):
    """No parameters needed — discovers series from BdE's published catalog CSVs."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

BDE_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` carries the upstream long-form prose. Lifted into
        # DESCRIPTION (rather than METADATA) so the embedder sees it at index
        # time — semantic recall on full sentences matters more than for
        # categorical metadata.
        Column(name="description", role=ColumnRole.DESCRIPTION),
        # ``source`` lets agents dispatch the right fetch connector when more
        # than one BdE source is wired. Today only ``bde_biest`` exists; the
        # column is in place so adding (e.g.) an SDMX path later costs zero
        # schema churn.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="alias", role=ColumnRole.METADATA),
        Column(name="dataset", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="unit", role=ColumnRole.METADATA),
        Column(name="decimals", role=ColumnRole.METADATA),
        Column(name="start_date", role=ColumnRole.METADATA),
        Column(name="end_date", role=ColumnRole.METADATA),
        Column(name="n_obs", role=ColumnRole.METADATA),
        Column(name="source_org", role=ColumnRole.METADATA),
    ]
)

BDE_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="key", role=ColumnRole.KEY, param_key="key", namespace="bde"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
        Column(name="value", dtype="numeric", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_bde_response(json_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Parse BdE JSON response into a long-format DataFrame.

    Each element in json_data represents one series with parallel
    fechas (dates) and valores (values) arrays.
    """
    all_rows: list[dict[str, Any]] = []

    for series in json_data:
        key = series.get("serie", "")
        title = series.get("descripcionCorta", series.get("descripcion", key))
        series.get("codFrecuencia", "")
        dates = series.get("fechas", [])
        values = series.get("valores", [])

        if not dates or not values:
            continue

        for date_str, raw_value in zip(dates, values, strict=False):
            try:
                value = float(raw_value) if raw_value not in (None, "", "NaN") else None
            except (ValueError, TypeError):
                value = None

            # Parse ISO datetime: "2024-01-31T00:00:00Z" → date
            date_val = date_str
            if isinstance(date_str, str) and "T" in date_str:
                with contextlib.suppress(ValueError):
                    date_val = datetime.strptime(date_str[:10], "%Y-%m-%d").strftime("%Y-%m-%d")

            all_rows.append(
                {
                    "key": key,
                    "title": title,
                    "date": date_val,
                    "value": value,
                }
            )

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=["key", "title", "date", "value"])


def _split_title_path(raw: str) -> tuple[str, str]:
    """Split a "/"-separated BdE title-path into (dataset, leaf_title).

    BdE encodes a taxonomic path in the title column. Two shapes occur:

    1. **Short path** (most TI/TC/PB/SI rows) — e.g.
       ``"Monetary policy/Eurosystem operations/Fixed rate auctions"``. The
       leaf is the most specific bit and makes a good catalog title; the
       prefix becomes ``dataset`` METADATA so agents can filter by family.
    2. **Long faceted path** (most BE rows) — e.g.
       ``"Descripción de la DSD: ... / Metodología: ... / Año Base: ... /
       Tipo de Transformación: ..."``. Each segment is a faceted ``key: value``
       attribute, and the "leaf" is just the last facet — meaningless on its
       own. In this case we leave the title to the caller (who falls back to
       ``description``) and put the whole faceted string in ``dataset``.

    Heuristic for "faceted path": all segments contain ``:``. That distinguishes
    BdE's DSD-encoded series (where every segment is ``Facet: value``) from
    natural-language taxonomies (where slashes separate concept names).
    """
    if "/" not in raw:
        return "", raw.strip()
    parts = [p.strip() for p in raw.split("/") if p.strip()]
    if not parts:
        return "", raw.strip()
    if len(parts) == 1:
        return "", parts[0]
    if all(":" in p for p in parts):
        # Faceted DSD-encoded path — no semantic leaf.
        return " › ".join(parts), ""
    return " › ".join(parts[:-1]), parts[-1]


def _parse_catalog_csv(text: str, *, category: str) -> list[dict[str, str]]:
    """Parse one ``catalogo_*.csv`` payload into enumerator rows.

    The CSV is comma-delimited, double-quoted, with a 17-column header BdE
    has been stable on for years. We keep the raw ``serie`` (the API code)
    as ``key`` and pull every descriptive column we can use for retrieval.
    Empty/whitespace-only rows are skipped; rows missing the ``serie`` key
    can't be fetched and are filtered out here.
    """
    reader = csv.reader(io.StringIO(text))
    rows: list[dict[str, str]] = []

    header_seen = False
    for raw_row in reader:
        if not raw_row:
            continue
        if not header_seen:
            # First row is the schema; skip it. We rely on positional access.
            header_seen = True
            continue
        if len(raw_row) < len(_CSV_HEADERS):
            # Defensive: BdE has occasional malformed rows when their export
            # job clips a description containing a literal ``"``. Skip rather
            # than crash the enumerator.
            logger.debug(
                "skipping malformed BdE catalog row (got %d cols, expected %d)",
                len(raw_row),
                len(_CSV_HEADERS),
            )
            continue

        record = dict(zip(_CSV_HEADERS, raw_row, strict=False))
        serie = (record.get("serie") or "").strip()
        if not serie:
            continue

        title_raw = (record.get("title") or "").strip()
        dataset, leaf_title = _split_title_path(title_raw)
        # Catalog title is the leaf if we have one, otherwise fall back to the
        # description (always populated) so semantic_text is never empty.
        title = leaf_title or (record.get("description") or "").strip() or serie

        freq_raw = (record.get("frequency_raw") or "").strip().upper()
        frequency = _FREQ_MAP_RAW.get(freq_raw, freq_raw.title() if freq_raw else "")

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


async def _fetch_catalog_chapter(
    client: httpx.AsyncClient,
    chapter: str,
    category: str,
) -> list[dict[str, str]]:
    """Fetch one ``catalogo_*.csv`` and return its parsed rows.

    Per-chapter failures are logged and degrade to an empty list rather than
    aborting the whole enumeration — so a transient outage on the bank lending
    survey CSV doesn't lose the 11k+ general statistics rows.
    """
    url = f"{_CATALOG_CSV_BASE_URL}/catalogo_{chapter}.csv"
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("BdE catalog chapter %r unavailable: %s", chapter, exc)
        return []
    # CSVs are CP1252-encoded; httpx's auto-detect can pick the wrong one if
    # BdE forgets to send a charset header, so decode explicitly.
    raw_bytes = response.content
    try:
        text = raw_bytes.decode(_CSV_ENCODING)
    except UnicodeDecodeError:
        # Fall back to latin-1 (strictly bigger than cp1252 in coverage); the
        # only diff is a handful of typographic glyphs we don't index on.
        text = raw_bytes.decode("latin-1", errors="replace")
    return _parse_catalog_csv(text, category=category)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=BDE_FETCH_OUTPUT, tags=["macro", "es"])
async def bde_fetch(params: BdeFetchParams) -> Result:
    """Fetch Banco de España time series by series code(s).

    Uses the BdE REST API (BIEST). Returns date + value with series metadata.
    """
    url = f"{_BASE_URL}/listaSeries"

    # BdE API: fetch each series individually and merge results.
    # Multi-series in a single request is unreliable (412 errors).
    keys = [k.strip() for k in params.key.split(",") if k.strip()]
    json_data: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        for key in keys:
            req_params: dict[str, str] = {
                "idioma": params.lang,
                "series": key,
            }
            if params.time_range is not None:
                req_params["rango"] = str(params.time_range)

            response = await client.get(url, params=req_params)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                map_http_error(exc, provider="bde", op_name="series")
            data = response.json()
            if isinstance(data, list):
                json_data.extend(data)

    if not isinstance(json_data, list) or not json_data:
        raise EmptyDataError(provider="bde", message=f"BdE returned empty or invalid response for: {params.key}")

    df = _parse_bde_response(json_data)
    if df.empty:
        raise EmptyDataError(provider="bde", message=f"No observations parsed for: {params.key}")

    return Result.from_dataframe(
        df,
        Provenance(
            source="bde",
            params={"key": params.key, "time_range": params.time_range},
            properties={
                "source_url": "https://www.bde.es/webbe/en/estadisticas/recursos/api-estadisticas-bde.html",
            },
        ),
    )


@enumerator(
    output=BDE_ENUMERATE_OUTPUT,
    tags=["macro", "es"],
)
async def enumerate_bde(params: BdeEnumerateParams) -> pd.DataFrame:
    """Enumerate every BdE statistical series across the seven published catalog chapters.

    BdE has no list endpoint or SDMX feed. The only authoritative discovery
    surface is the ``catalogo_{be,cf,ie,pb,si,tc,ti}.csv`` files BdE publishes
    alongside its statistical bulletin. Each row maps onto a series the
    ``/listaSeries`` API can fetch by ``serie`` code, and carries the
    descriptive prose, frequency, units, and date range needed to rank it
    in semantic search. Per-chapter network failures degrade gracefully —
    the enumerator returns whatever chapters succeeded rather than empty.
    Some ``serie`` codes appear in more than one chapter (e.g. a national
    accounts series listed under both BE and CF); we keep all occurrences so
    agents filtering by ``category`` see the series under every taxonomy it
    belongs to. Downstream de-duplication by ``key`` is the caller's call.
    """
    rows: list[dict[str, str]] = []
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for chapter, category in _CATALOG_CHAPTERS:
            chapter_rows = await _fetch_catalog_chapter(client, chapter, category)
            rows.extend(chapter_rows)

    columns = [
        "key",
        "title",
        "description",
        "source",
        "alias",
        "dataset",
        "category",
        "frequency",
        "unit",
        "decimals",
        "start_date",
        "end_date",
        "n_obs",
        "source_org",
    ]
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_bde.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    BDE_SEARCH_OUTPUT,
    PARSIMONY_BDE_CATALOG_URL_ENV,
    BdeSearchParams,
    bde_search,
)

CATALOGS: list[tuple[str, object]] = [("bde", enumerate_bde)]

CONNECTORS = Connectors([bde_fetch, enumerate_bde, bde_search])
