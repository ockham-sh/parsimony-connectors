"""The five Riksbank fetch verbs — one per API product, all keyless JSON.

Each verb maps a catalog hit back to a live request:

* ``riksbank_fetch`` — SWEA series by id (interest & exchange rates).
* ``riksbank_swestr_fetch`` — SWESTR fixing / compounded average / index.
* ``riksbank_monetary_policy_fetch`` — a forecast/outcome series across policy rounds.
* ``riksbank_turnover_fetch`` — a ``(market, frequency)`` turnover dataset.
* ``riksbank_holdings_fetch`` — a securities-holdings dataset.

All five accept an optional ``api_key`` that only raises the keyless quota.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony import Namespace
from parsimony.connector import connector
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError
from parsimony.transport import HttpClient
from parsimony.transport.helpers import fetch_json

from parsimony_riksbank import _http, holdings, monetary_policy, swea, swestr, turnover
from parsimony_riksbank.outputs import (
    RIKSBANK_FETCH_OUTPUT,
    RIKSBANK_HOLDINGS_FETCH_OUTPUT,
    RIKSBANK_MONETARY_POLICY_FETCH_OUTPUT,
    RIKSBANK_SWESTR_FETCH_OUTPUT,
    RIKSBANK_TURNOVER_FETCH_OUTPUT,
)

logger = logging.getLogger(__name__)


def _validate_date_pair(from_date: str | None, to_date: str | None) -> None:
    """Require both ``from_date`` and ``to_date`` together, or neither.

    ``from_date`` alone is ambiguous against the window-vs-latest dispatch, so exactly
    one of the pair raises :class:`InvalidParameterError`.
    """
    if (from_date is None) != (to_date is None):
        raise InvalidParameterError("riksbank", "Provide both from_date and to_date, or neither")


def _resolve_swea_title(http: HttpClient, series_id: str) -> str:
    """Best-effort title for a SWEA series from ``/Series`` (``shortDescription``).

    The ``/Observations`` payload carries only ``date``/``value``, so the label needs a
    secondary ``/Series`` request. This enrichment is non-essential: a transient
    operational failure (rate-limit, timeout, 5xx) of the secondary request must not
    fail the fetch, so the typed :class:`ConnectorError` family is caught and the id
    used as a fall-back. Programmer errors still propagate.
    """
    try:
        series_list = fetch_json(http, path="Series", op_name="Series")
    except ConnectorError as exc:
        logger.warning("riksbank: title lookup for %s failed (%s); using id", series_id, type(exc).__name__)
        return series_id
    if isinstance(series_list, dict):
        series_list = [series_list]
    if not isinstance(series_list, list):
        return series_id
    for s in series_list:
        if isinstance(s, dict) and s.get("seriesId") == series_id:
            return swea.series_title(s, series_id)
    return series_id


@connector(output=RIKSBANK_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_fetch(
    series_id: Annotated[str, Namespace("riksbank")],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a single Riksbank SWEA time series (interest or exchange rate) by series_id.

    Returns date + value with the upstream series name as title. Use ``from_date`` /
    ``to_date`` together to fetch a window; omit both to receive the latest observation.
    SWEA is keyless — ``api_key`` is optional and only raises the quota.

    **Rate limit**: the keyless quota is tight (~3 requests/burst). When fetching multiple
    series in a loop, add ``time.sleep(1.5)`` between calls to avoid HTTP 429 errors.
    """
    series_id = series_id.strip()
    if not series_id:
        raise InvalidParameterError("riksbank", "series_id must be non-empty")
    _validate_date_pair(from_date, to_date)

    http = _http.swea_client(api_key)
    if from_date and to_date:
        path = f"Observations/{series_id}/{from_date}/{to_date}"
    else:
        path = f"Observations/Latest/{series_id}"

    data = fetch_json(http, path=path, op_name="Observations")
    title = _resolve_swea_title(http, series_id)
    rows = swea.parse_observations(series_id, title, data)
    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No observations returned for: {series_id}",
            query_params={"series_id": series_id, "from_date": from_date, "to_date": to_date},
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


@connector(output=RIKSBANK_SWESTR_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_swestr_fetch(
    series: Annotated[swestr.SwestrSeries, Namespace("riksbank")],
    from_date: str | None = None,
    to_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a SWESTR fixing / compounded average / index series.

    Dispatches across three URL families: ``SWESTR`` -> ``/latest`` or ``/all``;
    ``SWESTRAVG*`` -> ``/avg/...``; ``SWESTRINDEX`` -> ``/index/...``. ``from_date`` /
    ``to_date`` together request a window; omit both for the latest published value.
    SWESTR was first published 2021-09-01, so a ``from_date`` earlier than that
    returns only the available observations (the effective start is the earliest
    ``date`` in the returned frame). Returns date + value plus SWESTR's native
    metadata (publication time, percentiles, transaction volumes) on additional
    columns. Keyless — ``api_key`` is optional.
    """
    if series not in swestr.SWESTR_IDS:
        raise InvalidParameterError(
            "riksbank", f"Unknown SWESTR series {series!r}; expected one of {sorted(swestr.SWESTR_IDS)}"
        )
    _validate_date_pair(from_date, to_date)

    http = _http.swestr_client(api_key)
    kind = swestr.swestr_kind(series)
    if from_date and to_date:
        query: dict[str, Any] | None = {"fromDate": from_date, "toDate": to_date}
        path = f"all/{series}" if kind == "rate" else f"{kind}/{series}"
    else:
        query = None
        path = f"latest/{series}" if kind == "rate" else f"{kind}/latest/{series}"

    payload = fetch_json(http, path=path, params=query, op_name=path)
    rows = swestr.parse_swestr_rows(series, payload)
    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No SWESTR observations returned for series={series!r}",
            query_params={"series": series, "from_date": from_date, "to_date": to_date},
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["title"] = swestr.TITLE_BY_ID.get(series, series)
    return df


@connector(output=RIKSBANK_MONETARY_POLICY_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_monetary_policy_fetch(
    series: Annotated[str, Namespace("riksbank")],
    policy_round: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a Riksbank Monetary Policy forecast/outcome series.

    ``series`` is a forecast series id (e.g. ``SEQGDPNAYSA`` for GDP, annual % change).
    ``policy_round`` selects one forecast vintage (e.g. ``2026:1``); omit it to return
    **every** published vintage, with a ``policy_round`` column disambiguating them.
    Each vintage's observations carry realised history up to the forecast cutoff plus the
    forecast horizon. Returns date + value plus ``policy_round`` and
    ``forecast_cutoff_date``. Keyless — ``api_key`` is optional.
    """
    series = series.strip()
    if not series:
        raise InvalidParameterError("riksbank", "series must be non-empty")

    # Policy-round names contain a colon (``2026:1``) and the gateway 404s if it is
    # percent-encoded — so this reads through the colon-safe raw helper rather than the
    # shared client's httpx param encoder (which would emit ``%3A``).
    query_items: dict[str, str] = {"series": series}
    if policy_round:
        query_items["policy_round_name"] = policy_round
    payload = _http.get_json_literal_query(
        f"{_http.MONETARY_POLICY_BASE}/forecasts", query_items, api_key=api_key, op_name="forecasts"
    )

    rows, title = monetary_policy.parse_forecast_rows(payload)
    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No monetary-policy observations returned for series={series!r}",
            query_params={"series": series, "policy_round": policy_round},
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["title"] = title or series
    return df


@connector(output=RIKSBANK_TURNOVER_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_turnover_fetch(
    market: Annotated[turnover.TurnoverMarket, Namespace("riksbank")],
    frequency: turnover.TurnoverFrequency = "monthly",
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a Riksbank turnover-statistics dataset for one market and frequency.

    ``market`` is ``fi`` (fixed income), ``fx`` (foreign exchange) or ``ird`` (interest
    rate derivatives); ``frequency`` is ``daily`` or ``monthly``. Returns a tidy long
    table: ``period`` + ``amount`` plus the facet columns
    ``asset`` / ``contract`` / ``counterparty``. Keyless — ``api_key`` is optional.

    Coverage is whatever the upstream dataset serves and there are no date
    parameters — for some markets/frequencies (e.g. fx and fi monthly) that is a
    limited rolling window of recent months, not the full history.
    """
    if market not in turnover.MARKETS:
        raise InvalidParameterError(
            "riksbank", f"Unknown market {market!r}; expected one of {sorted(turnover.MARKETS)}"
        )
    if frequency not in turnover.FREQUENCIES:
        raise InvalidParameterError(
            "riksbank", f"Unknown frequency {frequency!r}; expected one of {sorted(turnover.FREQUENCIES)}"
        )

    http = _http.turnover_client(api_key)
    path = f"markets/{market}/frequencies/{frequency}"
    payload = fetch_json(http, path=path, op_name=path)

    rows = turnover.parse_turnover_rows(market, frequency, payload)
    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No turnover data returned for market={market!r} frequency={frequency!r}",
            query_params={"market": market, "frequency": frequency},
        )
    df = pd.DataFrame(rows)
    df["period"] = pd.to_datetime(df["period"])
    df["title"] = f"Turnover — {market} ({frequency})"
    return df


@connector(output=RIKSBANK_HOLDINGS_FETCH_OUTPUT, tags=["macro", "se"], secrets=("api_key",))
def riksbank_holdings_fetch(
    dataset: Annotated[holdings.HoldingsDataset, Namespace("riksbank")],
    start_date: str | None = None,
    end_date: str | None = None,
    api_key: str = "",
) -> pd.DataFrame:
    """Fetch a Riksbank securities-holdings dataset.

    ``dataset`` is ``swedish_securities`` (per-ISIN detail) or
    ``swedish_securities_aggregated`` (summed by security group). ``start_date`` /
    ``end_date`` bound the window (``YYYY-MM-DD``); ``start_date`` defaults to the 2015
    programme inception. Returns ``date`` + ``balance_nominal_number`` plus the
    descriptive columns (security group, issuer, ISIN, maturity). Keyless — ``api_key``
    is optional.
    """
    if dataset not in holdings.DATASETS:
        raise InvalidParameterError(
            "riksbank", f"Unknown dataset {dataset!r}; expected one of {sorted(holdings.DATASETS)}"
        )

    http = _http.holdings_client(api_key)
    params = {"start_date": start_date or holdings.DEFAULT_START_DATE, "end_date": end_date}
    payload = fetch_json(http, path=dataset, params=params, op_name=f"holdings/{dataset}")

    rows = holdings.parse_holdings_rows(dataset, payload)
    if not rows:
        raise EmptyDataError(
            "riksbank",
            message=f"No holdings data returned for dataset={dataset!r}",
            query_params={"dataset": dataset, "start_date": start_date, "end_date": end_date},
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["title"] = dataset
    return df
