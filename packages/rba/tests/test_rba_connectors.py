"""Happy-path tests for the RBA connectors.

RBA scrapes the tables page for CSV links then fetches each CSV. No api_key;
the fetch path goes through ``_http_get`` which tries curl_cffi then falls
back to httpx. Tests mock httpx (the fallback) since curl_cffi is optional.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from parsimony_rba import CONNECTORS, RbaFetchParams, rba_fetch

_TABLES_HTML = """
<html><body>
<a href="/statistics/tables/csv/f1-data.csv">F1 Interest Rates</a>
<a href="/statistics/tables/csv/g1-data.csv">G1 Exchange Rates</a>
</body></html>
"""

_F1_CSV = (
    "F1 INTEREST RATES\n"
    ",Cash Rate Target\n"
    "Title,Cash Rate Target\n"
    "Description,Official cash rate target set by the RBA Board\n"
    "Frequency,Daily\n"
    "Units,Per cent\n"
    "Series ID,FIRMMCRTD\n"
    "01-Jan-2026,4.35\n"
    "02-Jan-2026,4.35\n"
)


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"rba_fetch", "enumerate_rba"}


@respx.mock
@pytest.mark.asyncio
async def test_rba_fetch_resolves_then_parses_csv() -> None:
    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/f1-data.csv").mock(
        return_value=httpx.Response(200, text=_F1_CSV)
    )

    result = await rba_fetch(RbaFetchParams(table_id="f1-data"))

    assert result.provenance.source == "rba"
    df = result.data
    assert "table_id" in df.columns
    assert df.iloc[0]["table_id"] == "f1-data"


@respx.mock
@pytest.mark.asyncio
async def test_rba_fetch_raises_value_error_for_unknown_table() -> None:
    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML)
    )

    with pytest.raises(ValueError, match="not found"):
        await rba_fetch(RbaFetchParams(table_id="nonexistent-table"))


def test_fetch_normalises_trailing_csv_suffix() -> None:
    p = RbaFetchParams(table_id="F1-DATA.csv")
    assert p.table_id == "f1-data"
