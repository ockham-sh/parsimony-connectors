"""Live integration tests for parsimony-fred.

Hits the real ``https://api.stlouisfed.org/fred`` endpoint. Skipped by
default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/fred -m integration

Requires ``FRED_API_KEY`` in the environment (workspace contributors
get this from ``ockham/.env`` via direnv; CI sets it from secrets).
"""

from __future__ import annotations

import pytest
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_fred import FredFetchParams, FredSearchParams, fred_fetch, fred_search

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_fred_search_unemployment_returns_unrate() -> None:
    creds = require_env("FRED_API_KEY")
    bound = fred_search.bind(api_key=creds["FRED_API_KEY"])

    result = await bound(FredSearchParams(search_text="unemployment rate"))

    assert_provenance_shape(result, expected_source="fred", required_param_keys=["search_text"])
    df = result.data
    assert not df.empty, "FRED search returned empty DataFrame for 'unemployment rate'"
    # UNRATE is the canonical FRED series for US unemployment — if the search
    # doesn't surface it at all, the connector is broken.
    assert "UNRATE" in set(df["id"]), f"UNRATE missing from search results: {list(df['id'])[:10]}"

    # Structural secret-redaction check — the real key was used but must not
    # round-trip into provenance or serialised output.
    assert_no_secret_leak(result, secret=creds["FRED_API_KEY"])


@pytest.mark.asyncio
async def test_fred_fetch_unrate_returns_observations() -> None:
    creds = require_env("FRED_API_KEY")
    bound = fred_fetch.bind(api_key=creds["FRED_API_KEY"])

    result = await bound(FredFetchParams(series_id="UNRATE"))

    assert_provenance_shape(result, expected_source="fred", required_param_keys=["series_id"])
    df = result.data
    assert not df.empty, "FRED fetch returned empty DataFrame for UNRATE"
    # Observations carry a date and a value at minimum.
    assert {"date", "value"}.issubset(df.columns), f"Missing date/value columns: {df.columns.tolist()}"
    assert len(df) > 100, f"UNRATE is monthly since 1948; expected >100 obs, got {len(df)}"

    assert_no_secret_leak(result, secret=creds["FRED_API_KEY"])
