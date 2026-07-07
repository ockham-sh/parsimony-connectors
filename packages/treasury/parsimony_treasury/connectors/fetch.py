"""US Treasury fetch connectors — Fiscal Data JSON + ODM rate-feed XML."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_treasury import _http, parsing
from parsimony_treasury.outputs import TREASURY_FETCH_OUTPUT, TREASURY_RATES_FETCH_OUTPUT
from parsimony_treasury.rate_feeds import RATE_FEED_NAMES, TreasuryRateFeed


@connector(output=TREASURY_FETCH_OUTPUT, tags=["macro", "us"])
def treasury_fetch(
    endpoint: Annotated[str, Namespace("treasury")],
    filter: str | None = None,
    sort: str | None = None,
    page_size: int = 100,
) -> pd.DataFrame:
    """Fetch US Treasury Fiscal Data by endpoint (e.g. ``v2/accounting/od/debt_to_penny``).

    Returns the dataset as a DataFrame with ``record_date`` parsed and metadata-typed
    numeric columns coerced. Optional ``filter`` (e.g. ``record_date:gte:2024-01-01``) and
    ``sort`` (e.g. ``-record_date``) pass through to the API; ``page_size`` (1–10000) caps
    the rows returned.
    """
    endpoint = endpoint.strip().lstrip("/")
    if not endpoint:
        raise InvalidParameterError("treasury", "endpoint must be non-empty")
    if page_size < 1 or page_size > 10000:
        raise InvalidParameterError("treasury", "page_size must be between 1 and 10000")

    req_params: dict[str, Any] = {"page[size]": page_size, "filter": filter, "sort": sort}
    body = fetch_json(
        _http.fiscal_client(),
        path=endpoint,
        params=req_params,
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
    df = parsing.coerce_fiscal_numeric(df, data_types)

    # Identity columns for the KEY/TITLE schema slots. ``labels`` maps a field name to its
    # human label; Fiscal Data labels ``record_date`` with the dataset's display name.
    df["endpoint"] = endpoint
    df["title"] = labels.get("record_date", endpoint)
    return df


@connector(output=TREASURY_RATES_FETCH_OUTPUT, tags=["macro", "us"])
def treasury_rates_fetch(
    feed: Annotated[TreasuryRateFeed, Namespace("treasury")],
    year: int | None = None,
) -> pd.DataFrame:
    """Fetch a Treasury Office of Debt Management rate feed for one calendar year.

    ``feed`` is one of: ``daily_treasury_yield_curve``, ``daily_treasury_real_yield_curve``,
    ``daily_treasury_bill_rates``, ``daily_treasury_long_term_rate``,
    ``daily_treasury_real_long_term``. The home.treasury.gov OData/Atom XML feed is
    paginated by year via ``field_tdr_date_value``; ``year=None`` defaults to the current
    UTC year. Returns a DataFrame whose columns are the feed's native rate columns (e.g.
    ``BC_10YEAR`` for the par yield curve) plus a normalised ``record_date``.
    """
    if feed not in RATE_FEED_NAMES:
        raise InvalidParameterError(
            "treasury",
            f"unknown rate feed {feed!r}; expected one of {sorted(RATE_FEED_NAMES)}",
        )
    resolved_year = year if year is not None else datetime.now(tz=UTC).year
    if resolved_year < 1990 or resolved_year > 2100:
        raise InvalidParameterError("treasury", "year must be between 1990 and 2100")

    op_name = f"rates/{feed}/{resolved_year}"
    xml_text = _http.get_text(
        _http.rates_client(),
        _http.RATES_PATH,
        params={"data": feed, "field_tdr_date_value": str(resolved_year)},
        op_name=op_name,
    )

    df = parsing.parse_treasury_rates_xml(xml_text)
    if df.empty:
        raise EmptyDataError(
            "treasury",
            message=f"No rows returned for rate feed {feed!r} year={resolved_year}",
            query_params={"feed": feed, "year": resolved_year},
        )

    df["feed"] = feed
    df["title"] = feed.replace("_", " ").title()
    df["source_url"] = f"{_http.RATES_BASE_URL}?data={feed}&field_tdr_date_value={resolved_year}"
    return df
