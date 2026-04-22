"""Happy-path tests for the Riksbank connectors.

Riksbank exposes an optional Ocp-Apim-Subscription-Key header; the connector
defaults ``api_key=""`` (quota lower without a key but the endpoint works).
Template 401/429 contract targets keyword-only deps that are required —
Riksbank's api_key is optional, so we don't exercise the 401/429 mapping
here.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_riksbank import (
    CONNECTORS,
    RiksbankFetchParams,
    riksbank_fetch,
)


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"riksbank_fetch", "enumerate_riksbank"}


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_fetch_returns_observations() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"date": "2026-04-17", "value": 11.35},
                {"date": "2026-04-18", "value": 11.40},
            ],
        )
    )
    # /Series title lookup — returned but optional
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(
        return_value=httpx.Response(
            200,
            json=[{"seriesId": "SEKEURPMI", "seriesName": "SEK/EUR exchange rate"}],
        )
    )

    bound = riksbank_fetch.bind(api_key="")
    result = await bound(RiksbankFetchParams(series_id="SEKEURPMI"))

    assert result.provenance.source == "riksbank"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "SEK/EUR exchange rate"


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/XX").mock(
        return_value=httpx.Response(200, json=[])
    )

    bound = riksbank_fetch.bind(api_key="")
    with pytest.raises(EmptyDataError):
        await bound(RiksbankFetchParams(series_id="XX"))


def test_fetch_rejects_empty_series_id() -> None:
    with pytest.raises(ValueError):
        RiksbankFetchParams(series_id="   ")


# NOTE: the existing `_both_dates_or_neither` validator is decorated with
# @field_validator but does not pass validate_default=True, so it does not
# fire when `to_date` takes its None default with `from_date` set. That is a
# pre-existing bug — documented here rather than fixed mid-sweep to keep
# per-package commits focused on migration-only changes.
