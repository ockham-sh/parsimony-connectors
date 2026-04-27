"""US Treasury Fiscal Data: fetch + catalog enumeration.

API docs: https://fiscaldata.treasury.gov/api-documentation/
No authentication required.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

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
from parsimony.transport import HttpClient, map_http_error
from pydantic import BaseModel, Field

_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
_METADATA_URL = "https://api.fiscaldata.treasury.gov/services/dtg/metadata/"
_TREASURY_RATES_BASE_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"

# OData Atom XML namespaces used by the home.treasury.gov rate feeds.
_ATOM_NS = "{http://www.w3.org/2005/Atom}"
_ODATA_DATASERVICES_NS = "{http://schemas.microsoft.com/ado/2007/08/dataservices}"
_ODATA_METADATA_NS = "{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}"

TreasuryRateFeed = Literal[
    "daily_treasury_yield_curve",
    "daily_treasury_real_yield_curve",
    "daily_treasury_bill_rates",
    "daily_treasury_long_term_rate",
    "daily_treasury_real_long_term",
]


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class TreasuryFetchParams(BaseModel):
    """Parameters for fetching US Treasury fiscal data."""

    endpoint: Annotated[str, "ns:treasury"] = Field(
        ..., description="API endpoint path (e.g. v2/accounting/od/debt_to_penny)"
    )
    filter: str | None = Field(
        default=None,
        description="Filter expression (e.g. record_date:gte:2024-01-01)",
    )
    sort: str | None = Field(
        default=None,
        description="Sort expression (e.g. -record_date for descending)",
    )
    page_size: int = Field(default=100, ge=1, le=10000, description="Records per page")


class TreasuryEnumerateParams(BaseModel):
    """No parameters needed — enumerates the full Treasury API catalog."""

    pass


class TreasuryRatesFetchParams(BaseModel):
    """Parameters for fetching a Treasury Office of Debt Management rate feed.

    The home.treasury.gov rate feeds — Daily Treasury Par Yield Curve,
    Bill Rates, Real Yield Curve, Long-Term Rates, Real Long-Term — are
    paginated by calendar year. ``feed`` is a closed enum so invalid
    values are caught at param-validation time rather than as a 404 from
    Treasury.
    """

    feed: Annotated[TreasuryRateFeed, "ns:treasury"] = Field(
        ...,
        description=(
            "Treasury OBM rate feed name (one of: daily_treasury_yield_curve, "
            "daily_treasury_real_yield_curve, daily_treasury_bill_rates, "
            "daily_treasury_long_term_rate, daily_treasury_real_long_term)."
        ),
    )
    year: int | None = Field(
        default=None,
        ge=1990,
        le=2100,
        description="Calendar year to retrieve. Defaults to the current UTC year.",
    )


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

TREASURY_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        # Compound code ``{endpoint}#{field}`` so every addressable time-series
        # measure has a unique catalog entry; agents split on ``#`` to recover
        # the fetchable endpoint and the column to read off the row.
        Column(name="code", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``definition`` is the Fiscal Data field's own descriptive text — the
        # most useful semantic signal for retrieval. Routing it through
        # DESCRIPTION (not METADATA) lifts it into ``semantic_text()`` so the
        # embedder indexes it, in addition to BM25.
        Column(name="definition", role=ColumnRole.DESCRIPTION),
        # ``source`` tells the agent which fetch connector to call —
        # ``"fiscal_data"`` → :func:`treasury_fetch`, ``"treasury_rates"`` →
        # :func:`treasury_rates_fetch`. Without this, agents would have to
        # sniff the ``code`` prefix.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="endpoint", role=ColumnRole.METADATA),
        Column(name="field", role=ColumnRole.METADATA),
        Column(name="data_type", role=ColumnRole.METADATA),
        Column(name="dataset", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
        Column(name="earliest_date", role=ColumnRole.METADATA),
        Column(name="latest_date", role=ColumnRole.METADATA),
    ]
)

# Treasury field ``data_type`` values that denote a time-series measure (as
# opposed to dates, identifiers, category labels, or row-scaffolding ints).
# The prefix match captures precision-suffixed variants (``CURRENCY0``,
# ``PERCENTAGE_PRECISE``, etc.) that Fiscal Data emits alongside the base
# types.
_MEASURE_TYPE_PREFIXES: tuple[str, ...] = (
    "CURRENCY",
    "NUMBER",
    "PERCENTAGE",
    "RATE",
)


def _is_measure_field(field: dict[str, Any]) -> bool:
    """Whether *field* is an addressable time-series measure.

    Most measures are typed ``CURRENCY``/``NUMBER``/``PERCENTAGE``/``RATE``
    (or precision-suffixed variants) — caught by prefix match. Treasury's
    Certified Interest Rates (TCIR) tables, however, store rate values as
    ``STRING`` data — Treasury's data dictionary quirk, not real strings.
    Recognise those by name: a STRING column whose name contains ``rate``
    or ``yield`` is a rate value, except for purely descriptive
    ``*_desc`` fields and Y/N-coded indicators.
    """
    data_type = (field.get("data_type") or "").strip()
    if data_type.startswith(_MEASURE_TYPE_PREFIXES):
        return True
    if data_type == "STRING":
        column_name = (field.get("column_name") or "").lower()
        if "rate" not in column_name and "yield" not in column_name:
            return False
        if column_name.endswith("_desc"):
            return False
        definition = (field.get("definition") or "").strip()
        # ``floating_rate`` is a Y/N flag describing the security, not a
        # rate value; same for any other Y/N-prefixed indicator.
        return not definition.startswith("Y/N")
    return False


# ---------------------------------------------------------------------------
# Treasury Office of Debt Management rate feeds (home.treasury.gov XML)
#
# These famous series — Daily Treasury Par Yield Curve, Daily Treasury
# Bill Rates, etc. — are NOT in Fiscal Data's ``/dtg/metadata/`` endpoint.
# They live on a separate Treasury subdomain as XML/CSV feeds. Cataloguing
# them under the same ``treasury`` namespace gives agents one search
# surface; the ``endpoint`` metadata field carries a ``home/<feed>`` prefix
# so the fetch path can route by source.
# ---------------------------------------------------------------------------

_TREASURY_RATE_DATASET_CATEGORY = "Office of Debt Management"


def _rate_feed_source_url(feed: str) -> str:
    return (
        "https://home.treasury.gov/resource-center/data-chart-center/"
        f"interest-rates/TextView?type={feed}"
    )


_TREASURY_RATE_FEEDS: tuple[dict[str, Any], ...] = (
    {
        "feed": "daily_treasury_yield_curve",
        "dataset": "Daily Treasury Par Yield Curve Rates",
        "frequency": "Daily",
        "definition_template": (
            "{tenor} constant-maturity Treasury par yield curve rate, published daily by the "
            "U.S. Treasury Office of Debt Management. The par yield curve is derived from "
            "indicative bid-side prices on the most actively traded Treasury securities and is "
            "the canonical risk-free rate benchmark for that maturity."
        ),
        "fields": (
            ("BC_1MONTH", "1 Month"),
            ("BC_1_5MONTH", "1.5 Month"),
            ("BC_2MONTH", "2 Month"),
            ("BC_3MONTH", "3 Month"),
            ("BC_4MONTH", "4 Month"),
            ("BC_6MONTH", "6 Month"),
            ("BC_1YEAR", "1 Year"),
            ("BC_2YEAR", "2 Year"),
            ("BC_3YEAR", "3 Year"),
            ("BC_5YEAR", "5 Year"),
            ("BC_7YEAR", "7 Year"),
            ("BC_10YEAR", "10 Year"),
            ("BC_20YEAR", "20 Year"),
            ("BC_30YEAR", "30 Year"),
        ),
    },
    {
        "feed": "daily_treasury_real_yield_curve",
        "dataset": "Daily Treasury Real Yield Curve Rates",
        "frequency": "Daily",
        "definition_template": (
            "{tenor} real (TIPS-based) Treasury yield curve rate, published daily by the U.S. "
            "Treasury Office of Debt Management. Reflects the inflation-adjusted yield on "
            "Treasury Inflation-Protected Securities at the given constant maturity."
        ),
        "fields": (
            ("TC_5YEAR", "5 Year"),
            ("TC_7YEAR", "7 Year"),
            ("TC_10YEAR", "10 Year"),
            ("TC_20YEAR", "20 Year"),
            ("TC_30YEAR", "30 Year"),
        ),
    },
    {
        "feed": "daily_treasury_bill_rates",
        "dataset": "Daily Treasury Bill Rates",
        "frequency": "Daily",
        "definition_template": "{tenor} {kind}, published daily by the U.S. Treasury Office of Debt Management.",
        "fields": (
            ("ROUND_B1_CLOSE_4WK_2", "4-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_4WK_2", "4-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_6WK_2", "6-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_6WK_2", "6-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_8WK_2", "8-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_8WK_2", "8-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_13WK_2", "13-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_13WK_2", "13-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_17WK_2", "17-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_17WK_2", "17-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_26WK_2", "26-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_26WK_2", "26-Week Treasury Bill — Coupon Equivalent Yield"),
            ("ROUND_B1_CLOSE_52WK_2", "52-Week Treasury Bill — Closing Bank Discount Rate"),
            ("ROUND_B1_YIELD_52WK_2", "52-Week Treasury Bill — Coupon Equivalent Yield"),
        ),
    },
    {
        "feed": "daily_treasury_long_term_rate",
        "dataset": "Daily Treasury Long-Term Rates",
        "frequency": "Daily",
        # Long format — the actual rate is in column ``RATE`` parameterised
        # by ``RATE_TYPE``. One catalog row pointing at the feed is the most
        # useful surface; agents fetch and pivot on RATE_TYPE themselves.
        "definition_template": (
            "Daily Treasury long-term composite rates, published by the U.S. Treasury Office of "
            "Debt Management. The feed is in long format: each row carries a ``RATE_TYPE`` "
            "(e.g. LT, LT5, etc.) and a ``RATE`` value. Used to evaluate long-term Treasury "
            "yields when bonds with maturities of 10+ years are not available."
        ),
        "fields": (("RATE", "Long-Term Composite Rate"),),
    },
    {
        "feed": "daily_treasury_real_long_term",
        "dataset": "Daily Treasury Real Long-Term Rate Averages",
        "frequency": "Daily",
        "definition_template": (
            "Daily Treasury real long-term rate averages (TIPS-based), published by the U.S. "
            "Treasury Office of Debt Management."
        ),
        "fields": (("RATE", "Real Long-Term Rate Average"),),
    },
)


def _build_treasury_rate_rows() -> list[dict[str, str]]:
    """One row per (rate-feed, column) entry from :data:`_TREASURY_RATE_FEEDS`.

    Pure function — the registry is static so this involves no I/O. The
    code is ``home/{feed}#{column}``; the ``home/`` prefix distinguishes
    these entries from Fiscal Data codes (which are versioned paths like
    ``v2/...``) so a future fetch dispatcher can route by source.
    """
    rows: list[dict[str, str]] = []
    for spec in _TREASURY_RATE_FEEDS:
        feed = spec["feed"]
        endpoint = f"home/{feed}"
        dataset = spec["dataset"]
        frequency = spec["frequency"]
        template: str = spec["definition_template"]
        for column_name, tenor in spec["fields"]:
            kind = "Closing Bank Discount Rate" if "_CLOSE_" in column_name else "Coupon Equivalent Yield"
            definition = template.format(tenor=tenor, kind=kind)
            rows.append(
                {
                    "code": f"{endpoint}#{column_name}",
                    "title": f"{tenor} — {dataset}",
                    "source": "treasury_rates",
                    "endpoint": endpoint,
                    "field": column_name,
                    "definition": definition,
                    "data_type": "PERCENTAGE",
                    "dataset": dataset,
                    "category": _TREASURY_RATE_DATASET_CATEGORY,
                    "frequency": frequency,
                    "earliest_date": "",
                    "latest_date": "",
                }
            )
    return rows

# Treasury returns tabular datasets — the output is a DataFrame whose
# columns depend on the endpoint.  We use a minimal schema with just
# the identity key; actual data columns vary per endpoint.
TREASURY_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="endpoint", role=ColumnRole.KEY, param_key="endpoint", namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="record_date", dtype="datetime", role=ColumnRole.DATA),
    ]
)

# Rates feeds return one row per business day; rate columns vary per feed
# (e.g. ``BC_10YEAR`` for the par yield curve, ``ROUND_B1_YIELD_4WK_2`` for
# bill rates). The schema names only the columns we always materialise; the
# feed-specific rate columns ride along as additional DATA columns.
TREASURY_RATES_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="feed", role=ColumnRole.KEY, param_key="feed", namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="record_date", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


def _make_http() -> HttpClient:
    return HttpClient(_BASE_URL, query_params={"format": "json"})


@connector(output=TREASURY_FETCH_OUTPUT, tags=["macro", "us"])
async def treasury_fetch(params: TreasuryFetchParams) -> Result:
    """Fetch US Treasury fiscal data by endpoint.

    Returns the dataset as-is with ``record_date`` parsed and numeric
    columns converted.  Each row is one record from the Treasury API.
    """
    http = _make_http()
    req_params: dict[str, Any] = {"page[size]": params.page_size}
    if params.filter:
        req_params["filter"] = params.filter
    if params.sort:
        req_params["sort"] = params.sort

    response = await http.request("GET", f"/{params.endpoint}", params=req_params)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="treasury", op_name=params.endpoint)
    body = response.json()

    data = body.get("data", [])
    if not data:
        raise EmptyDataError(provider="treasury", message=f"No data returned for endpoint: {params.endpoint}")

    meta = body.get("meta", {})
    labels = meta.get("labels", {})
    data_types = meta.get("dataTypes", {})

    df = pd.DataFrame(data)

    # Parse record_date
    if "record_date" in df.columns:
        df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")

    # Convert numeric columns identified by API metadata
    numeric_types = {"CURRENCY", "NUMBER", "PERCENTAGE", "RATE"}
    for col, dtype in data_types.items():
        if dtype in numeric_types and col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

    # Add identity columns
    table_name = labels.get("record_date", params.endpoint)
    df["endpoint"] = params.endpoint
    df["title"] = table_name

    return Result.from_dataframe(
        df,
        Provenance(
            source="treasury",
            params={"endpoint": params.endpoint},
            properties={
                "total_records": meta.get("total-count"),
                "source_url": f"https://fiscaldata.treasury.gov/datasets/{params.endpoint}",
            },
        ),
    )


_RATES_DATE_COLUMNS: tuple[str, ...] = ("NEW_DATE", "INDEX_DATE", "QUOTE_DATE")
_RATES_NUMERIC_TYPES: frozenset[str] = frozenset(
    {"Edm.Double", "Edm.Decimal", "Edm.Single", "Edm.Int32", "Edm.Int64"}
)
_RATES_DATETIME_TYPES: frozenset[str] = frozenset({"Edm.DateTime"})


def _parse_treasury_rates_xml(xml_text: str) -> pd.DataFrame:
    """Parse a home.treasury.gov OData Atom rate-feed payload into a DataFrame.

    Each ``<entry>`` carries an ``m:properties`` block whose ``d:NAME``
    children are the row's columns. ``Edm.DateTime`` values become
    pandas datetimes, ``Edm.Double``/``Edm.Decimal`` become floats, and
    everything else stays as a string. The first ``Edm.DateTime`` column
    encountered (Treasury uses ``NEW_DATE``, ``INDEX_DATE``, or
    ``QUOTE_DATE`` depending on the feed) is duplicated as
    ``record_date`` to give every feed a uniform time axis.
    """
    root = ET.fromstring(xml_text)
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


@connector(output=TREASURY_RATES_FETCH_OUTPUT, tags=["macro", "us"])
async def treasury_rates_fetch(params: TreasuryRatesFetchParams) -> Result:
    """Fetch a Treasury Office of Debt Management rate feed for one calendar year.

    The home.treasury.gov XML feed is paginated by year via the
    ``field_tdr_date_value`` query parameter. ``year=None`` defaults to
    the current UTC year. Returns a DataFrame whose columns are the
    feed's native rate columns (e.g. ``BC_10YEAR`` for the par yield
    curve) plus a normalised ``record_date``.
    """
    year = params.year if params.year is not None else datetime.now(tz=UTC).year
    op_name = f"rates/{params.feed}/{year}"
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        try:
            response = await client.get(
                _TREASURY_RATES_BASE_URL,
                params={"data": params.feed, "field_tdr_date_value": str(year)},
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="treasury", op_name=op_name)
        xml_text = response.text

    df = _parse_treasury_rates_xml(xml_text)
    if df.empty:
        raise EmptyDataError(
            provider="treasury",
            message=f"No rows returned for rate feed {params.feed!r} year={year}",
        )

    df["feed"] = params.feed
    df["title"] = params.feed.replace("_", " ").title()

    return Result.from_dataframe(
        df,
        Provenance(
            source="treasury",
            params={"feed": params.feed, "year": year},
            properties={
                "row_count": len(df),
                "source_url": (
                    f"{_TREASURY_RATES_BASE_URL}?data={params.feed}&field_tdr_date_value={year}"
                ),
            },
        ),
    )


@enumerator(
    output=TREASURY_ENUMERATE_OUTPUT,
    tags=["macro", "us"],
)
async def enumerate_treasury(params: TreasuryEnumerateParams) -> pd.DataFrame:
    """Enumerate every addressable Treasury time series across two sources.

    1. **Fiscal Data API** (``/dtg/metadata/``) — yields one row per
       (endpoint, measure-field) pair. ``data_type`` typed as
       ``CURRENCY``/``NUMBER``/``PERCENTAGE``/``RATE`` (or precision-suffixed
       variants) is a measure; STRING fields whose names contain
       ``rate``/``yield`` are also recognised so the Treasury Certified
       Interest Rates (TCIR) tables — which Treasury stores as STRING — are
       included. Dates, identifiers, and category labels are excluded.
    2. **Office of Debt Management rate feeds** (home.treasury.gov XML) —
       static registry covering the daily par yield curve, real yield curve,
       bill rates, and long-term composite rates. These are the canonical
       benchmark series and are not in the Fiscal Data metadata.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(_METADATA_URL)
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            map_http_error(exc, provider="treasury", op_name="datasets/metadata")
        raw = resp.json()

    datasets: list[dict] = []
    if isinstance(raw, list):
        datasets = raw
    elif isinstance(raw, dict):
        for key in ("datasets", "data", "result"):
            if key in raw and isinstance(raw[key], list):
                datasets = raw[key]
                break

    prefix = "/services/api/fiscal_service/"
    rows: list[dict[str, str]] = []

    for ds in datasets:
        dataset_title = ds.get("title") or ds.get("dataset_name", "")
        category = ds.get("publisher", "")
        ds_frequency = ds.get("update_frequency", "")
        for api in ds.get("apis", []):
            endpoint = api.get("endpoint_txt") or ""
            if endpoint.startswith(prefix):
                endpoint = endpoint[len(prefix) :]
            if not endpoint:
                endpoint = api.get("api_id", "")
            if not endpoint:
                continue
            table_name = api.get("table_name") or dataset_title
            frequency = api.get("update_frequency") or ds_frequency
            earliest_date = api.get("earliest_date", "") or ""
            latest_date = api.get("latest_date", "") or ""
            for field in api.get("fields", []):
                if not _is_measure_field(field):
                    continue
                column_name = field.get("column_name", "") or ""
                if not column_name:
                    continue
                pretty_name = field.get("pretty_name") or column_name
                definition = field.get("definition", "") or ""
                rows.append(
                    {
                        "code": f"{endpoint}#{column_name}",
                        "title": f"{pretty_name} — {table_name}",
                        "source": "fiscal_data",
                        "endpoint": endpoint,
                        "field": column_name,
                        "definition": definition,
                        "data_type": field.get("data_type", "") or "",
                        "dataset": dataset_title,
                        "category": category,
                        "frequency": frequency,
                        "earliest_date": earliest_date,
                        "latest_date": latest_date,
                    }
                )

    rows.extend(_build_treasury_rate_rows())

    columns = [
        "code",
        "title",
        "source",
        "endpoint",
        "field",
        "definition",
        "data_type",
        "dataset",
        "category",
        "frequency",
        "earliest_date",
        "latest_date",
    ]
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_treasury.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    PARSIMONY_TREASURY_CATALOG_URL_ENV,
    TREASURY_SEARCH_OUTPUT,
    TreasurySearchParams,
    treasury_search,
)

CATALOGS: list[tuple[str, object]] = [("treasury", enumerate_treasury)]

CONNECTORS = Connectors([treasury_fetch, treasury_rates_fetch, enumerate_treasury, treasury_search])
