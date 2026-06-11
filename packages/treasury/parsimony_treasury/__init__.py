"""US Treasury connectors for parsimony.

US Treasury is a **keyless** public source — no API key, no ``secrets=``, no
``bind()``/``load()``. It spans three transports:

* the **Fiscal Data** JSON API (``api.fiscaldata.treasury.gov``) — fetched via
  :func:`parsimony.transport.helpers.fetch_json` over a plain
  :func:`make_http_client` client;
* the **Office of Debt Management** rate feeds (``home.treasury.gov``) — served
  as OData/Atom **XML**, so they cannot use ``fetch_json`` (GET + JSON only) and
  go through a raw :class:`~parsimony.transport.HttpClient` + the reusable
  :func:`_get_text` helper (§6.7: ``request("GET")`` + ``raise_for_status()`` +
  ``map_http_error`` **and** ``map_timeout_error``), then a stdlib
  ``xml.etree.ElementTree`` parse;
* the published **semantic-search catalog** (``treasury_search``) — delegates to
  :func:`parsimony.catalog.search.make_local_search_connector`.

Exports :data:`CONNECTORS`:

* ``treasury_fetch`` (``@connector``) — any Fiscal Data endpoint as a DataFrame.
* ``treasury_rates_fetch`` (``@connector``) — one ODM rate feed (XML) by year.
* ``enumerate_treasury`` (``@enumerator``) — discover Fiscal Data measures + ODM
  rate-feed benchmarks for catalog indexing.
* ``treasury_search`` (``@connector``) — semantic search over the catalog.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, get_args

import httpx
import pandas as pd
from parsimony import Namespace
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
)
from parsimony.transport import HttpClient, map_http_error, map_timeout_error
from parsimony.transport.helpers import fetch_json, make_http_client

_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
_METADATA_BASE = "https://api.fiscaldata.treasury.gov/services/dtg"
# home.treasury.gov rate feeds: split the host from the path so the request
# URL carries no trailing slash. The bare ``.../xml/`` form 301-redirects to
# ``.../xml`` on every call — wasteful — so we target the canonical path.
_TREASURY_RATES_HOST = "https://home.treasury.gov"
_TREASURY_RATES_PATH = "/resource-center/data-chart-center/interest-rates/pages/xml"
_TREASURY_RATES_BASE_URL = f"{_TREASURY_RATES_HOST}{_TREASURY_RATES_PATH}"

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

_RATE_FEED_NAMES: frozenset[str] = frozenset(get_args(TreasuryRateFeed))


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
        # most useful semantic signal for retrieval.
        Column(name="definition", role=ColumnRole.METADATA),
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

_ENUMERATE_COLUMNS = [c.name for c in TREASURY_ENUMERATE_OUTPUT.columns]

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
        Column(name="endpoint", role=ColumnRole.KEY, namespace="treasury"),
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
        Column(name="feed", role=ColumnRole.KEY, namespace="treasury"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="record_date", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


def _fiscal_http() -> HttpClient:
    """Build the keyless Fiscal Data JSON client (``format=json`` on every call)."""
    return make_http_client(_BASE_URL, query_params={"format": "json"})


def _metadata_http() -> HttpClient:
    """Build the keyless Fiscal Data metadata client."""
    return make_http_client(_METADATA_BASE)


def _rates_http() -> HttpClient:
    """Build the keyless home.treasury.gov XML rate-feed client.

    The feeds emit OData/Atom XML (not JSON), so they are read with
    :func:`_get_text` rather than ``fetch_json``. The client targets the host;
    the canonical (trailing-slash-free) feed path is passed per request.
    """
    return make_http_client(_TREASURY_RATES_HOST, timeout=30.0)


def _get_text(http: HttpClient, path: str, *, params: dict[str, Any], op_name: str) -> str:
    """GET *path* and return the raw text body (non-JSON / XML document).

    The §6.7 raw-transport shape for any response ``fetch_json`` can't handle:
    ``request("GET")`` + ``raise_for_status()`` mapping **both**
    ``HTTPStatusError`` (via :func:`map_http_error`) **and** ``TimeoutException``
    (via :func:`map_timeout_error`). Reusable across the XML/text-feed
    connectors (bde/snb/destatis will reuse this pattern).
    """
    try:
        response = http.request("GET", path, params=params)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="treasury", op_name=op_name)
    except httpx.TimeoutException as exc:
        map_timeout_error(exc, provider="treasury", op_name=op_name)
    return response.text


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=TREASURY_FETCH_OUTPUT, tags=["macro", "us"])
def treasury_fetch(
    endpoint: Annotated[str, Namespace("treasury")],
    filter: str | None = None,
    sort: str | None = None,
    page_size: int = 100,
) -> pd.DataFrame:
    """Fetch US Treasury Fiscal Data by endpoint (e.g. ``v2/accounting/od/debt_to_penny``).

    Returns the dataset as a DataFrame with ``record_date`` parsed and
    metadata-typed numeric columns converted. Optional ``filter`` (e.g.
    ``record_date:gte:2024-01-01``) and ``sort`` (e.g. ``-record_date``)
    pass through to the API; ``page_size`` (1–10000) caps the rows returned.
    """
    endpoint = endpoint.strip().lstrip("/")
    if not endpoint:
        raise InvalidParameterError("treasury", "endpoint must be non-empty")
    if page_size < 1 or page_size > 10000:
        raise InvalidParameterError("treasury", "page_size must be between 1 and 10000")

    req_params: dict[str, Any] = {
        "page[size]": page_size,
        "filter": filter,
        "sort": sort,
    }
    body = fetch_json(
        _fiscal_http(),
        path=endpoint,
        params=req_params,
        provider="treasury",
        op_name=endpoint,
    )

    if not isinstance(body, dict):
        raise ParseError("treasury", f"unexpected response shape for endpoint {endpoint!r}")
    if "data" not in body:
        raise ParseError("treasury", f"response missing 'data' for endpoint {endpoint!r}")

    data = body.get("data", [])
    if not data:
        raise EmptyDataError(
            "treasury",
            message=f"No data returned for endpoint: {endpoint}",
            query_params={"endpoint": endpoint, "filter": filter, "sort": sort},
        )

    meta = body.get("meta", {})
    labels = meta.get("labels", {}) if isinstance(meta, dict) else {}
    data_types = meta.get("dataTypes", {}) if isinstance(meta, dict) else {}

    df = pd.DataFrame(data)

    if "record_date" in df.columns:
        df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")

    # Convert only the columns the API metadata types as numeric measures —
    # never blanket-coerce (that would NaN string identifiers/labels).
    numeric_types = {"CURRENCY", "NUMBER", "PERCENTAGE", "RATE"}
    for col, dtype in data_types.items():
        if dtype in numeric_types and col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

    # Identity columns for the KEY/TITLE schema slots. ``labels`` maps a
    # field name to its human label; Fiscal Data labels ``record_date`` with
    # the dataset's display name, which doubles as a serviceable title.
    df["endpoint"] = endpoint
    df["title"] = labels.get("record_date", endpoint)

    return df


_RATES_DATE_COLUMNS: tuple[str, ...] = ("NEW_DATE", "INDEX_DATE", "QUOTE_DATE")
_RATES_NUMERIC_TYPES: frozenset[str] = frozenset({"Edm.Double", "Edm.Decimal", "Edm.Single", "Edm.Int32", "Edm.Int64"})
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

    Raises :class:`ParseError` if *xml_text* is not well-formed XML (a 200
    that is not the expected Atom shape).
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


@connector(output=TREASURY_RATES_FETCH_OUTPUT, tags=["macro", "us"])
def treasury_rates_fetch(
    feed: Annotated[TreasuryRateFeed, Namespace("treasury")],
    year: int | None = None,
) -> pd.DataFrame:
    """Fetch a Treasury Office of Debt Management rate feed for one calendar year.

    ``feed`` is one of: ``daily_treasury_yield_curve``,
    ``daily_treasury_real_yield_curve``, ``daily_treasury_bill_rates``,
    ``daily_treasury_long_term_rate``, ``daily_treasury_real_long_term``. The
    home.treasury.gov OData/Atom XML feed is paginated by year via
    ``field_tdr_date_value``; ``year=None`` defaults to the current UTC year.
    Returns a DataFrame whose columns are the feed's native rate columns (e.g.
    ``BC_10YEAR`` for the par yield curve) plus a normalised ``record_date``.
    """
    if feed not in _RATE_FEED_NAMES:
        raise InvalidParameterError(
            "treasury",
            f"unknown rate feed {feed!r}; expected one of {sorted(_RATE_FEED_NAMES)}",
        )
    resolved_year = year if year is not None else datetime.now(tz=UTC).year
    if resolved_year < 1990 or resolved_year > 2100:
        raise InvalidParameterError("treasury", "year must be between 1990 and 2100")

    op_name = f"rates/{feed}/{resolved_year}"
    xml_text = _get_text(
        _rates_http(),
        _TREASURY_RATES_PATH,
        params={"data": feed, "field_tdr_date_value": str(resolved_year)},
        op_name=op_name,
    )

    df = _parse_treasury_rates_xml(xml_text)
    if df.empty:
        raise EmptyDataError(
            "treasury",
            message=f"No rows returned for rate feed {feed!r} year={resolved_year}",
            query_params={"feed": feed, "year": resolved_year},
        )

    df["feed"] = feed
    df["title"] = feed.replace("_", " ").title()
    df["source_url"] = f"{_TREASURY_RATES_BASE_URL}?data={feed}&field_tdr_date_value={resolved_year}"
    return df


@enumerator(output=TREASURY_ENUMERATE_OUTPUT, tags=["macro", "us"])
def enumerate_treasury() -> pd.DataFrame:
    """Enumerate Treasury Fiscal Data measures and ODM rate-feed benchmarks.

    Combines Fiscal Data metadata rows (one per addressable time-series
    measure across every dataset) with the static Office of Debt Management
    yield and bill-rate series, for catalog indexing. Each row is an
    ``{endpoint}#{field}`` code with a title, definition, and routing source.
    """
    raw = fetch_json(
        _metadata_http(),
        path="metadata/",
        provider="treasury",
        op_name="datasets/metadata",
    )

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

    # @enumerator drops unmapped columns then requires an EXACT match to the
    # declared schema — build the frame with exactly the declared columns.
    df = pd.DataFrame(rows, columns=_ENUMERATE_COLUMNS) if rows else pd.DataFrame(columns=_ENUMERATE_COLUMNS)
    return df


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_treasury.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    PARSIMONY_TREASURY_CATALOG_URL_ENV,
    TREASURY_SEARCH_OUTPUT,
    treasury_search,
)

CONNECTORS = Connectors([treasury_fetch, treasury_rates_fetch, enumerate_treasury, treasury_search])


def load(*, catalog_url: str | None = None) -> Connectors:
    """Return :data:`CONNECTORS` with an optional catalog URL bound on search."""
    if catalog_url is None:
        return CONNECTORS
    return CONNECTORS.bind(catalog_url=catalog_url)
