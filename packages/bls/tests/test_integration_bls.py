"""Live integration tests for parsimony-bls.

Hits the real ``https://api.bls.gov/publicAPI/v2`` endpoint. Skipped by
default. Run with ``uv run pytest packages/bls -m integration``.

``BLS_API_KEY`` is optional (raises rate limits); the tests bind it when
present and still run without it.
"""

from __future__ import annotations

import os

import pytest
from parsimony_test_support import assert_no_secret_leak, assert_provenance_shape

from parsimony_bls import bls_fetch, enumerate_bls

pytestmark = pytest.mark.integration

_KEY = os.environ.get("BLS_API_KEY", "")


def test_bls_fetch_unemployment() -> None:
    bound = bls_fetch.bind(api_key=_KEY)

    # LNS14000000 is the canonical US unemployment rate series from BLS.
    result = bound(series_id="LNS14000000", start_year="2025", end_year="2026")

    assert_provenance_shape(result, expected_source="bls_fetch", required_param_keys=["series_id"])
    df = result.data
    assert not df.empty, "BLS fetch of LNS14000000 returned empty DataFrame"
    assert {"date", "value"}.issubset(df.columns), f"missing date/value: {df.columns.tolist()}"
    assert df["value"].notna().any(), "value column is entirely NaN"
    if _KEY:
        # With a key, BLS enables the catalog → title is a real series title,
        # not the series-id fallback. (Keyless, the catalog is disabled.)
        assert df["title"].iloc[0] != "LNS14000000", "title fell back to series_id despite a key"
        assert_no_secret_leak(result, secret=_KEY)


def test_enumerate_bls_single_survey() -> None:
    # Bound to ONE survey (CE = Current Employment Statistics) — cheap, no
    # full multi-survey fan-out.
    bound = enumerate_bls.bind(api_key=_KEY)

    result = bound(survey="CE")

    assert_provenance_shape(result, expected_source="enumerate_bls")
    df = result.data
    assert not df.empty, "BLS CE-survey enumeration returned empty DataFrame"
    assert list(df.columns) == ["series_id", "title", "survey"]
    assert df["series_id"].str.len().gt(0).all(), "blank series_id in enumeration"
    if _KEY:
        assert_no_secret_leak(result, secret=_KEY)
