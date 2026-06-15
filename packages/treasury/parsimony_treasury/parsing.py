"""Pure parsing for the US Treasury connectors — no I/O.

Three concerns:

* **Fiscal Data measure detection + row building** — which fields in the metadata
  endpoint are addressable time-series measures, and turning the dataset → apis →
  fields tree into one catalog row per measure.
* **ODM rate-feed XML parsing** — the OData/Atom feed → a DataFrame with a normalised
  ``record_date`` time axis.
* **Fetch-time numeric coercion** — coerce only the columns the API types as numeric
  measures (never blanket-coerce, which would NaN string identifiers/labels).
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pandas as pd
from parsimony.errors import ParseError

# ---------------------------------------------------------------------------
# Fiscal Data measure detection
# ---------------------------------------------------------------------------

# Field ``data_type`` prefixes that denote a time-series measure. Prefix match captures
# precision-suffixed variants (``CURRENCY0``, ``PERCENTAGE_PRECISE``, ``CURRENCY3``…).
# (``RATE`` was a dead prefix — no Fiscal Data field is typed RATE; removed 2026-06-09.)
# INTEGER is excluded: it is predominantly row scaffolding (``src_line_nbr`` dominates);
# the handful of genuine counts (``nbr_issues_accepted`` etc.) stay fetchable on the
# endpoint, just not catalogued as standalone measures.
MEASURE_TYPE_PREFIXES: tuple[str, ...] = ("CURRENCY", "NUMBER", "PERCENTAGE")

#: ``meta.dataTypes`` values (at fetch time) treated as numeric for coercion. Same prefix
#: set as the enumerator — the live feed returns base types today, but prefix-matching is
#: safe against a future suffixed type leaking onto the fetch path.
_FISCAL_NUMERIC_PREFIXES: tuple[str, ...] = MEASURE_TYPE_PREFIXES

_ENDPOINT_PREFIX = "/services/api/fiscal_service/"


def is_measure_field(field: dict[str, Any]) -> bool:
    """Whether *field* is an addressable Fiscal Data time-series measure.

    Most measures are typed ``CURRENCY``/``NUMBER``/``PERCENTAGE`` (or precision-suffixed
    variants) — caught by prefix match. Treasury's Certified Interest Rates (TCIR) tables,
    however, store rate values as ``STRING`` (a data-dictionary quirk, not real strings).
    Recognise those by name: a STRING column whose name contains ``rate``/``yield`` is a
    rate value, except purely descriptive ``*_desc`` fields and Y/N-coded indicators.
    """
    data_type = (field.get("data_type") or "").strip()
    if data_type.startswith(MEASURE_TYPE_PREFIXES):
        return True
    if data_type == "STRING":
        column_name = (field.get("column_name") or "").lower()
        if "rate" not in column_name and "yield" not in column_name:
            return False
        if column_name.endswith("_desc"):
            return False
        # ``floating_rate`` is a Y/N flag describing the security, not a rate value.
        definition = (field.get("definition") or "").strip()
        return not definition.startswith("Y/N")
    return False


def fiscal_measure_rows(datasets: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Turn the metadata ``datasets`` tree into one catalog row per measure field.

    Pure: the I/O (fetching + unwrapping the metadata payload) lives in the enumerator.
    Datasets with no queryable ``endpoint_txt`` (static-file/PDF publications) contribute
    nothing — they have no JSON API to fetch.
    """
    rows: list[dict[str, str]] = []
    for ds in datasets:
        dataset_title = ds.get("title") or ds.get("dataset_name", "")
        category = ds.get("publisher", "")
        ds_frequency = ds.get("update_frequency", "")
        for api in ds.get("apis", []):
            endpoint = api.get("endpoint_txt") or ""
            if endpoint.startswith(_ENDPOINT_PREFIX):
                endpoint = endpoint[len(_ENDPOINT_PREFIX) :]
            if not endpoint:
                endpoint = api.get("api_id", "")
            if not endpoint:
                # No queryable path (static-file dataset) — nothing addressable.
                continue
            table_name = api.get("table_name") or dataset_title
            frequency = api.get("update_frequency") or ds_frequency
            earliest_date = api.get("earliest_date", "") or ""
            latest_date = api.get("latest_date", "") or ""
            for field in api.get("fields", []):
                if not is_measure_field(field):
                    continue
                column_name = field.get("column_name", "") or ""
                if not column_name:
                    continue
                pretty_name = field.get("pretty_name") or column_name
                rows.append(
                    {
                        "code": f"{endpoint}#{column_name}",
                        "title": f"{pretty_name} — {table_name}",
                        "source": "fiscal_data",
                        "endpoint": endpoint,
                        "field": column_name,
                        "description": field.get("definition", "") or "",
                        "data_type": field.get("data_type", "") or "",
                        "dataset": dataset_title,
                        "category": category,
                        "frequency": frequency,
                        "earliest_date": earliest_date,
                        "latest_date": latest_date,
                    }
                )
    return rows


def unwrap_metadata(raw: Any) -> list[dict[str, Any]]:
    """The metadata endpoint returns a top-level JSON *list* live, but historically wrapped
    the datasets under a ``datasets``/``data``/``result`` key — accept both shapes."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("datasets", "data", "result"):
            value = raw.get(key)
            if isinstance(value, list):
                return value
    return []


def coerce_fiscal_numeric(df: pd.DataFrame, data_types: dict[str, str]) -> pd.DataFrame:
    """Coerce only the columns the API metadata types as numeric measures (comma-stripped).

    Never blanket-coerce — that would NaN string identifiers / labels. Mutates a copy is
    unnecessary here (caller owns ``df``); we assign column-by-column in place.
    """
    for col, dtype in data_types.items():
        if isinstance(dtype, str) and dtype.startswith(_FISCAL_NUMERIC_PREFIXES) and col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )
    return df


# ---------------------------------------------------------------------------
# ODM rate-feed XML (OData/Atom)
# ---------------------------------------------------------------------------

_ATOM_NS = "{http://www.w3.org/2005/Atom}"
_ODATA_DATASERVICES_NS = "{http://schemas.microsoft.com/ado/2007/08/dataservices}"
_ODATA_METADATA_NS = "{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}"

#: The feed's time column, by feed: NEW_DATE (par/real curves), INDEX_DATE (bills),
#: QUOTE_DATE (long-term). First-present-wins into a uniform ``record_date``.
_RATES_DATE_COLUMNS: tuple[str, ...] = ("NEW_DATE", "INDEX_DATE", "QUOTE_DATE")
_RATES_NUMERIC_TYPES: frozenset[str] = frozenset(
    {"Edm.Double", "Edm.Decimal", "Edm.Single", "Edm.Int32", "Edm.Int64"}
)
_RATES_DATETIME_TYPES: frozenset[str] = frozenset({"Edm.DateTime"})


def parse_treasury_rates_xml(xml_text: str) -> pd.DataFrame:
    """Parse a home.treasury.gov OData/Atom rate-feed payload into a DataFrame.

    Each ``<entry>`` carries an ``m:properties`` block whose ``d:NAME`` children are the
    row's columns. ``Edm.DateTime`` → pandas datetime, numeric Edm types → float, else
    string. The first date column present (``NEW_DATE``/``INDEX_DATE``/``QUOTE_DATE``) is
    duplicated as ``record_date`` to give every feed a uniform, sorted time axis.

    Raises :class:`ParseError` if *xml_text* is not well-formed XML (a 200 that is not the
    expected Atom shape — e.g. an error/maintenance HTML page).
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ParseError("treasury", f"rate feed did not return parseable XML: {exc}") from exc

    rows: list[dict[str, Any]] = []
    for entry in root.findall(f"{_ATOM_NS}entry"):
        props = entry.find(f"{_ATOM_NS}content/{_ODATA_METADATA_NS}properties")
        if props is None:
            continue
        row: dict[str, Any] = {}
        for prop in props:
            tag = prop.tag.removeprefix(_ODATA_DATASERVICES_NS)
            edm_type = prop.attrib.get(f"{_ODATA_METADATA_NS}type", "Edm.String")
            text = prop.text
            if text is None or text == "":
                row[tag] = None
                continue
            if edm_type in _RATES_DATETIME_TYPES:
                row[tag] = pd.to_datetime(text, errors="coerce")
            elif edm_type in _RATES_NUMERIC_TYPES:
                try:
                    row[tag] = float(text)
                except (TypeError, ValueError):
                    row[tag] = None
            else:
                row[tag] = text
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for date_col in _RATES_DATE_COLUMNS:
        if date_col in df.columns:
            df["record_date"] = df[date_col]
            break
    if "record_date" in df.columns:
        df = df.sort_values("record_date").reset_index(drop=True)
    return df


__all__ = [
    "MEASURE_TYPE_PREFIXES",
    "is_measure_field",
    "fiscal_measure_rows",
    "unwrap_metadata",
    "coerce_fiscal_numeric",
    "parse_treasury_rates_xml",
]
