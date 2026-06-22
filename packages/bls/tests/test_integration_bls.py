"""Live integration tests for parsimony-bls.

Hits the real BLS Public Data API (``api.bls.gov``) and the Akamai-walled bulk
flat-file site (``download.bls.gov``, via curl_cffi). Skipped by default; run with
``uv run pytest packages/bls -m integration``.

``BLS_API_KEY`` is optional (raises quota); the tests bind it when present.
The series-catalog build is bounded to a small survey (JT = JOLTS) to stay quick.
"""

from __future__ import annotations

import os

import pytest
from parsimony_test_support import assert_provenance_shape

from parsimony_bls.catalog_build import build_series_catalog
from parsimony_bls.connectors.enumerate_series import enumerate_bls_series
from parsimony_bls.connectors.enumerate_surveys import enumerate_bls_surveys
from parsimony_bls.connectors.fetch import bls_fetch

pytestmark = pytest.mark.integration

_KEY = os.environ.get("BLS_API_KEY", "")
_SMALL_SURVEY = "JT"  # JOLTS -- small (~2k series) and title-less (exercises composition)


def test_bls_fetch_unemployment() -> None:
    bound = bls_fetch.bind(api_key=_KEY)
    # LNS14000000 is the canonical US unemployment rate series from BLS.
    result = bound(series_id="LNS14000000", start_year="2025", end_year="2026")
    assert_provenance_shape(result, expected_source="bls_fetch", required_param_keys=["series_id"])
    df = result.data
    assert not df.empty, "BLS fetch of LNS14000000 returned empty"
    assert {"date", "value"}.issubset(df.columns)
    assert df["value"].notna().any()


def test_bls_fetch_unemployment_live() -> None:
    bound = bls_fetch.bind(api_key=_KEY)
    result = bound(series_id="LNS14000000", start_year="2024", end_year="2026")
    assert_provenance_shape(result, expected_source="bls_fetch", required_param_keys=["series_id"])
    df = result.data
    assert not df.empty, "BLS fetch of LNS14000000 returned empty"
    assert {"date", "value"}.issubset(df.columns)
    assert df["value"].notna().any()


def test_enumerate_surveys_live() -> None:
    result = enumerate_bls_surveys.bind(api_key=_KEY)()
    df = result.data
    assert_provenance_shape(result, expected_source="enumerate_bls_surveys")
    assert len(df) >= 60, f"expected the full survey roster, got {len(df)}"
    assert "CU" in set(df["code"])


def test_enumerate_series_live_composes_titles() -> None:
    # Title-less survey: titles must be composed from dimension labels, not blank.
    result = enumerate_bls_series(survey=_SMALL_SURVEY)
    df = result.data
    assert not df.empty
    assert df["title"].str.len().gt(0).all(), "blank composed title in a title-less survey"
    assert (df["code"].str.startswith("JT")).all()
    # dimension metadata present for structured search
    assert any(c.endswith("_label") for c in df.columns)


def test_two_tier_build_and_search_live() -> None:
    catalog = build_series_catalog(_SMALL_SURVEY)
    assert len(catalog.entities) > 100
    hits = catalog.search("job openings", limit=5)
    assert hits, "expected JOLTS series for 'job openings'"
    assert all(h.code.startswith("JT") for h in hits)
