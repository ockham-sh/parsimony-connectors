"""Live integration tests for parsimony-eia.

Hits the real ``https://api.eia.gov/v2`` endpoint. Skipped by default
(root ``pyproject.toml`` sets ``-m 'not integration'``). Run with::

    uv run pytest packages/eia -m integration

Requires ``EIA_API_KEY`` (workspace contributors get it from ``ockham/.env``
via direnv; CI sets it from secrets).
"""

from __future__ import annotations

import pytest
from parsimony_test_support import (
    assert_no_secret_leak,
    assert_provenance_shape,
    require_env,
)

from parsimony_eia import eia_fetch, enumerate_eia

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_eia_fetch_petroleum_spot_prices() -> None:
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    # petroleum/pri/spt — spot prices — is a stable EIA v2 route.
    result = await bound(route="petroleum/pri/spt")

    assert_provenance_shape(result, expected_source="eia_fetch", required_param_keys=["route"])
    df = result.data
    assert not df.empty, "EIA fetch of petroleum/pri/spt returned empty DataFrame"
    assert {"period", "value"}.issubset(df.columns), f"missing period/value: {df.columns.tolist()}"
    # The measure must actually be populated, not all-NaN metadata rows.
    assert df["value"].notna().any(), "value column is entirely NaN — measure facet not returned"

    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


@pytest.mark.asyncio
async def test_eia_fetch_non_value_measure_route() -> None:
    # electricity/retail-sales has NO `value` measure — valid measures are
    # revenue/sales/price/customers. Exercises the route-specific `measure` param
    # + the normalize-to-`value` path (the default measure would 400 here).
    creds = require_env("EIA_API_KEY")
    bound = eia_fetch.bind(api_key=creds["EIA_API_KEY"])

    result = await bound(route="electricity/retail-sales", measure="price", frequency="annual")

    assert_provenance_shape(result, expected_source="eia_fetch", required_param_keys=["route", "measure"])
    df = result.data
    assert not df.empty, "electricity/retail-sales price fetch returned empty DataFrame"
    assert "value" in df.columns, f"measure not normalized to value: {df.columns.tolist()}"
    assert df["value"].notna().any(), "normalized value column is entirely NaN"

    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])


@pytest.mark.asyncio
async def test_enumerate_eia_lists_top_level_routes() -> None:
    creds = require_env("EIA_API_KEY")
    bound = enumerate_eia.bind(api_key=creds["EIA_API_KEY"])

    # Single request to the v2 root — cheap, no fan-out.
    result = await bound()

    assert_provenance_shape(result, expected_source="enumerate_eia")
    df = result.data
    assert not df.empty, "EIA route enumeration returned empty DataFrame"
    assert list(df.columns) == ["route", "title", "description"]
    # petroleum is a stable top-level EIA route.
    assert "petroleum" in set(df["route"]), f"petroleum missing from routes: {list(df['route'])}"
    # description must carry real text, not be an empty/constant column.
    assert df["description"].str.len().gt(0).any(), "description column is empty for every route"

    assert_no_secret_leak(result, secret=creds["EIA_API_KEY"])
