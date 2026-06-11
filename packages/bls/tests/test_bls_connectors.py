"""Happy-path tests for the BLS connectors.

BLS uses POST with JSON body and returns its own error codes in the response
body rather than HTTP 4xx. The api_key is optional; 401/429 error-mapping
tests from ``CONTRIBUTING.md §4`` §4 do not apply (key-bearing is
defined as a key-required dep, and BLS defaults ``api_key=""``).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, RateLimitError

from parsimony_bls import (
    CONNECTORS,
    bls_fetch,
    enumerate_bls,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bls_fetch", "enumerate_bls"}


# ---------------------------------------------------------------------------
# bls_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_bls_fetch_returns_series_observations() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {
                            "seriesID": "LNS14000000",
                            "catalog": {"series_title": "Unemployment Rate"},
                            "data": [
                                {"year": "2026", "period": "M03", "value": "4.1"},
                                {"year": "2026", "period": "M02", "value": "4.0"},
                            ],
                        }
                    ]
                },
            },
        )
    )

    result = bls_fetch(series_id="LNS14000000", start_year="2026", end_year="2026")

    assert result.provenance.source == "bls_fetch"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "Unemployment Rate"


@respx.mock
def test_bls_fetch_raises_parse_error_on_bls_status_failure() -> None:
    # BLS signals failure in the body with HTTP 200 — map a non-success status
    # (that isn't a quota threshold) to ParseError, NOT a fake status_code=0.
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={"status": "REQUEST_NOT_PROCESSED", "message": ["Invalid series ID"]},
        )
    )

    with pytest.raises(ParseError):
        bls_fetch(series_id="BAD", start_year="2026", end_year="2026")


@respx.mock
def test_bls_fetch_maps_threshold_to_rate_limit() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_NOT_PROCESSED",
                "message": ["Request could not be serviced, the daily threshold has been reached."],
            },
        )
    )

    with pytest.raises(RateLimitError) as exc_info:
        bls_fetch(series_id="LNS14000000", start_year="2026", end_year="2026")
    assert exc_info.value.quota_exhausted is True


@respx.mock
def test_bls_fetch_raises_empty_data_when_no_series_returned() -> None:
    respx.post("https://api.bls.gov/publicAPI/v2/timeseries/data/").mock(
        return_value=httpx.Response(
            200,
            json={"status": "REQUEST_SUCCEEDED", "Results": {"series": []}},
        )
    )

    with pytest.raises(EmptyDataError):
        bls_fetch(series_id="XYZ", start_year="2026", end_year="2026")


# ---------------------------------------------------------------------------
# enumerate_bls (bounded to one survey)
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_bls_single_survey() -> None:
    # With a survey code, no /surveys call is made — just the one popular list.
    respx.get("https://api.bls.gov/publicAPI/v2/timeseries/popular").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {"seriesID": "CES0000000001", "seriesTitle": "Total nonfarm employment"},
                        {"seriesID": "CES0500000003"},
                    ]
                },
            },
        )
    )

    bound = enumerate_bls.bind(api_key="test-key")
    result = bound(survey="CE")

    df = result.data
    assert list(df.columns) == ["series_id", "title", "survey"]
    assert set(df["series_id"]) == {"CES0000000001", "CES0500000003"}
    # title falls back to the series id when the popular payload omits it.
    assert df.set_index("series_id").loc["CES0500000003", "title"] == "CES0500000003"
    assert set(df["survey"]) == {"CE"}


# ---------------------------------------------------------------------------
# Parameter validation (inline — no separate param model)
# ---------------------------------------------------------------------------


def test_bls_fetch_rejects_non_four_digit_year() -> None:
    with pytest.raises(InvalidParameterError, match="start_year"):
        bls_fetch(series_id="LNS14000000", start_year="26", end_year="2026")


def test_bls_fetch_rejects_empty_series_id() -> None:
    with pytest.raises(InvalidParameterError, match="series_id"):
        bls_fetch(series_id="   ", start_year="2026", end_year="2026")
