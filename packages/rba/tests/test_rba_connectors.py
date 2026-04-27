"""Happy-path tests for the RBA connectors.

RBA scrapes the tables page for CSV links then fetches each CSV. No api_key;
the fetch path goes through ``_http_get`` which tries curl_cffi then falls
back to httpx. Tests mock httpx (the fallback) since curl_cffi is optional.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from parsimony_rba import (
    CONNECTORS,
    RbaEnumerateParams,
    RbaFetchParams,
    enumerate_rba,
    rba_fetch,
)

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

# Two tables that share a series id (``BFC5WDZ``) — mirrors the real-world
# B13.1.x / B13.2.x regional-bank breakdowns the audit identified as the
# main collision source. Used to assert the enumerator emits a compound
# ``code`` and keeps both rows distinct.
_TABLES_HTML_COLLISION = """
<html><body>
<a href="/statistics/tables/csv/b13-1-2-africa.csv">B13.1.2 Africa</a>
<a href="/statistics/tables/csv/b13-2-1-africa.csv">B13.2.1 Africa</a>
</body></html>
"""

_B13_1_2_CSV = (
    "B13.1.2 BANK CLAIMS — AFRICA\n"
    ",Bank Foreign Claims to Africa\n"
    "Title,Bank Foreign Claims to Africa\n"
    "Description,On-balance-sheet bank foreign claims on Africa and Middle East counterparties\n"
    "Frequency,Quarterly\n"
    "Units,$ million\n"
    "Series ID,BFC5WDZ\n"
    "31-Dec-2023,1100\n"
    "31-Mar-2024,1234\n"
)

_B13_2_1_CSV = (
    "B13.2.1 BANK CLAIMS — ULTIMATE-RISK BASIS\n"
    ",Ultimate-Risk Foreign Claims to Africa\n"
    "Title,Ultimate-Risk Foreign Claims to Africa\n"
    "Description,Ultimate-risk-basis bank foreign claims on Africa and Middle East counterparties\n"
    "Frequency,Quarterly\n"
    "Units,$ million\n"
    "Series ID,BFC5WDZ\n"
    "31-Dec-2023,5500\n"
    "31-Mar-2024,5678\n"
)


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"rba_fetch", "enumerate_rba", "rba_search"}


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


# ---------------------------------------------------------------------------
# enumerate_rba — catalog completeness
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_rba_emits_description_table_id_unit_and_source() -> None:
    """One row per Series ID with the full Treasury-grade column set:

    * compound ``code`` for KEY,
    * ``description`` (DESCRIPTION) populated from the CSV's Description row
      so the embedder sees it,
    * ``table_id`` and ``series_id`` exposed as METADATA,
    * ``unit`` captured from the Units row,
    * ``source`` set to ``"rba_csv"`` for dispatch consistency.
    """
    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/f1-data.csv").mock(
        return_value=httpx.Response(200, text=_F1_CSV)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/g1-data.csv").mock(
        return_value=httpx.Response(404, text="missing")
    )

    df = (await enumerate_rba(RbaEnumerateParams())).data

    # Schema completeness — every Treasury-grade column must be present.
    assert {
        "code",
        "title",
        "description",
        "source",
        "table_id",
        "series_id",
        "category",
        "frequency",
        "unit",
    } <= set(df.columns)

    # The single F1 series should have round-tripped end-to-end.
    f1 = df[df["series_id"] == "FIRMMCRTD"]
    assert len(f1) == 1
    row = f1.iloc[0]
    assert row["code"] == "f1-data#FIRMMCRTD"
    assert row["table_id"] == "f1-data"
    assert row["title"] == "Cash Rate Target"
    assert row["description"] == "Official cash rate target set by the RBA Board"
    assert row["unit"] == "Per cent"
    assert row["source"] == "rba_csv"
    assert row["category"] == "Interest Rates and Yields"
    assert row["frequency"] == "Daily"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_rba_compound_code_keeps_cross_table_series_id_collisions() -> None:
    """Series ids reused across tables (B13.1.x vs B13.2.x in the wild) must
    emit two distinct catalog rows, distinguished by ``table_id`` in the
    compound ``code``. Previously a bare ``series_id`` KEY silently dropped
    the second occurrence."""
    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML_COLLISION)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/b13-1-2-africa.csv").mock(
        return_value=httpx.Response(200, text=_B13_1_2_CSV)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/b13-2-1-africa.csv").mock(
        return_value=httpx.Response(200, text=_B13_2_1_CSV)
    )

    df = (await enumerate_rba(RbaEnumerateParams())).data

    same_sid = df[df["series_id"] == "BFC5WDZ"]
    assert len(same_sid) == 2, "shared series_id must produce two distinct entries"
    assert set(same_sid["code"]) == {
        "b13-1-2-africa#BFC5WDZ",
        "b13-2-1-africa#BFC5WDZ",
    }
    assert set(same_sid["table_id"]) == {"b13-1-2-africa", "b13-2-1-africa"}
    # Distinct descriptions survive — the most useful semantic signal for
    # disambiguating the two rows at search time.
    assert len(set(same_sid["description"])) == 2


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_rba_source_metadata_uniform() -> None:
    """Every emitted row carries ``source='rba_csv'`` so an agent dispatching
    off a search hit knows which fetch connector to call without parsing
    the code prefix — matches Treasury's ``source`` dispatch column."""
    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML_COLLISION)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/b13-1-2-africa.csv").mock(
        return_value=httpx.Response(200, text=_B13_1_2_CSV)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/b13-2-1-africa.csv").mock(
        return_value=httpx.Response(200, text=_B13_2_1_CSV)
    )

    df = (await enumerate_rba(RbaEnumerateParams())).data
    assert set(df["source"]) == {"rba_csv"}


# ---------------------------------------------------------------------------
# XLSX-exclusive sheet + xls-hist coverage
#
# Two secondary sources close gaps the CSV pass misses:
#
# * ``xls/a03.xlsx`` carries a ``Bond Purchase Program`` sheet whose 7
#   series never appear in the a3-* CSVs. The enumerator's
#   ``_XLSX_EXCLUSIVE_SHEETS`` allow-list keeps this intentional and
#   prevents re-emitting series already captured from CSVs.
# * ``xls-hist/*.xls`` legacy binaries hold discontinued series — b3
#   repo rates pre-2013, c9 cheque/card historicals, f16 retail interest,
#   etc. ~186 catalog rows that would otherwise be invisible.
#
# Both pipes share ``_metadata_from_header_rows``, so the XLSX and XLS
# tests also exercise the common parser contract.
# ---------------------------------------------------------------------------


def _make_xlsx_fixture(sheets: list[tuple[str, list[list[object]]]]) -> bytes:
    """Build an in-memory XLSX workbook with the given sheets and return its bytes."""
    import io

    import openpyxl

    wb = openpyxl.Workbook()
    # Remove the default sheet we don't want.
    wb.remove(wb.active)
    for name, rows in sheets:
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _rba_metadata_rows(
    title: str,
    description: str,
    unit: str,
    frequency: str,
    series_id: str,
    value: float,
) -> list[list[object]]:
    """Construct the RBA-shaped metadata header block (Title / Description /
    Frequency / Units / Series ID) plus a single data row. Mirrors the
    layout that both ``_parse_xlsx_exclusive`` and ``_parse_xls_hist`` key on.
    """
    return [
        ["SAMPLE TABLE HEADING", ""],
        ["Title", title],
        ["Description", description],
        ["Frequency", frequency],
        ["Type", "Original"],
        ["Units", unit],
        ["", ""],
        ["Source", "RBA"],
        ["Publication date", "2026-04-24"],
        ["Series ID", series_id],
        ["2026-01-01", value],
    ]


_TABLES_HTML_WITH_XLSX = """
<html><body>
<a href="/statistics/tables/csv/a1-data.csv">A1 Reserve Bank of Australia</a>
<a href="/statistics/tables/xls/a03.xlsx">A3 Open Market Operations</a>
</body></html>
"""


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_rba_pulls_xlsx_exclusive_sub_sheet() -> None:
    """``a03.xlsx``'s Bond Purchase Program sheet holds 7 series never
    republished as a CSV. The enumerator must pick them up and tag them
    ``source='rba_xlsx'`` so dispatch knows they came from the XLSX path
    rather than the CSV path. All other sheets are skipped to avoid
    duplicating content already captured from the CSVs.
    """
    xlsx_data = _make_xlsx_fixture(
        [
            (
                "ES Balances and Repo Agreements",
                # This sheet's series overlap with CSV content, so the
                # enumerator's allow-list must SKIP it to prevent
                # duplication even though it carries valid metadata.
                _rba_metadata_rows(
                    title="Should Be Skipped",
                    description="CSV-duplicate sheet",
                    unit="$m",
                    frequency="Daily",
                    series_id="CSVDUP1",
                    value=100.0,
                ),
            ),
            (
                "Bond Purchase Program",
                _rba_metadata_rows(
                    title="Face Value",
                    description="Face value of bonds purchased under the BPP",
                    unit="$ million",
                    frequency="As required",
                    series_id="ALDBPPFVD",
                    value=500.0,
                ),
            ),
            ("Notes", [["Notes"], ["Some notes text"]]),
        ]
    )

    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML_WITH_XLSX)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/a1-data.csv").mock(
        return_value=httpx.Response(200, text=_F1_CSV.replace("F1 INTEREST RATES", "A1 RESERVE BANK"))
    )
    respx.get("https://www.rba.gov.au/statistics/tables/xls/a03.xlsx").mock(
        return_value=httpx.Response(200, content=xlsx_data)
    )
    # Historical-data page is fetched but returns no useful links here.
    respx.get("https://www.rba.gov.au/statistics/historical-data.html").mock(
        return_value=httpx.Response(200, text="<html></html>")
    )

    df = (await enumerate_rba(RbaEnumerateParams())).data

    # Bond Purchase Program sheet's single series must be present and
    # tagged rba_xlsx; the allow-listed skip must keep the CSV-duplicate
    # sheet out.
    bpp = df[df["series_id"] == "ALDBPPFVD"]
    assert len(bpp) == 1, "Bond Purchase Program series must be emitted exactly once"
    row = bpp.iloc[0]
    assert row["source"] == "rba_xlsx"
    assert row["title"] == "Face Value"
    assert row["unit"] == "$ million"
    # Sheet name folded into table_id so rows stay unique across sheets.
    assert row["table_id"] == "a03/Bond Purchase Program"
    assert row["code"] == "a03/Bond Purchase Program#ALDBPPFVD"

    # The non-allow-listed sheet must NOT contribute a row.
    assert "CSVDUP1" not in set(df["series_id"])


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_rba_pulls_xls_hist_discontinued_series() -> None:
    """``xls-hist/*.xls`` legacy binaries expose ~186 discontinued series
    (b3 repo rates pre-2013, c9 cheque historicals, etc.) that dropped
    out of the live CSVs long ago. They're catalogged with
    ``source='rba_xlsx_hist'`` so search can still surface them and
    agents know to treat them as archival.

    xls-hist workbooks with multiple data sheets have the sheet name
    folded into ``table_id`` to keep compound codes unique across
    sheets (a03hist-2003-2008 has one sheet per bond line).
    """
    # An xls file can't be easily fabricated in pure Python, so mock the
    # inner parse function. What we're testing here is the ENUMERATOR's
    # orchestration — that it discovers the xls-hist link, fetches, and
    # merges the resulting rows without colliding with CSV rows.
    import parsimony_rba as pkg

    historical_html = """
    <html><body>
    <a href="/statistics/tables/xls-hist/b03hist.xls">B3 Repo Agreements (historical)</a>
    <a href="/statistics/tables/xls-hist/1983-1986.xls">Period archive (no series IDs)</a>
    </body></html>
    """

    def fake_parse(data: bytes, table_id: str) -> list[dict[str, str]]:
        # One "discontinued" series to stand in for the 28 real ones.
        return [
            {
                "code": f"{table_id}#REPO1D",
                "table_id": table_id,
                "series_id": "REPO1D",
                "title": "1-Day Repurchase Agreement (discontinued)",
                "description": "Average yield on 1-day repurchase agreements",
                "category": "Banking and Finance",
                "frequency": "Monthly",
                "unit": "Per cent",
                "source": "rba_xlsx_hist",
            }
        ]

    respx.get("https://www.rba.gov.au/statistics/tables/").mock(
        return_value=httpx.Response(200, text=_TABLES_HTML)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/f1-data.csv").mock(
        return_value=httpx.Response(200, text=_F1_CSV)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/csv/g1-data.csv").mock(
        return_value=httpx.Response(404, text="missing")
    )
    respx.get("https://www.rba.gov.au/statistics/historical-data.html").mock(
        return_value=httpx.Response(200, text=historical_html)
    )
    respx.get("https://www.rba.gov.au/statistics/tables/xls-hist/b03hist.xls").mock(
        return_value=httpx.Response(200, content=b"fake-xls-bytes")
    )
    # ``1983-1986.xls`` starts with a digit — the enumerator should skip it
    # entirely (period-range archives don't carry Series ID rows), so we
    # don't need a mock for that URL.

    orig = pkg._parse_xls_hist
    pkg._parse_xls_hist = fake_parse  # type: ignore[assignment]
    try:
        df = (await enumerate_rba(RbaEnumerateParams())).data
    finally:
        pkg._parse_xls_hist = orig  # type: ignore[assignment]

    hist_rows = df[df["source"] == "rba_xlsx_hist"]
    assert len(hist_rows) == 1
    row = hist_rows.iloc[0]
    assert row["series_id"] == "REPO1D"
    assert row["table_id"] == "b03hist"
    assert row["code"] == "b03hist#REPO1D"
    # CSV pass is still present and untouched.
    assert "FIRMMCRTD" in set(df["series_id"])


def test_metadata_from_header_rows_accepts_mnemonic_label() -> None:
    """The legacy ``xls-hist/zcr-analytical-series-hist.xls`` workbook
    labels its series ``Mnemonic`` instead of ``Series ID``. The shared
    parser must treat both tokens identically so that discontinued
    analytical feeds survive the enumerator pass.
    """
    from parsimony_rba import _metadata_from_header_rows

    sheet_rows: list[list[object]] = [
        ["ZCR ANALYTICAL SERIES", "", ""],
        ["Title", "Zero-coupon Forward Rate", "Zero-coupon Yield"],
        ["Description", "0Y forward zero-coupon rate", "0Y zero-coupon yield"],
        ["Frequency", "Daily", "Daily"],
        ["Units", "Per cent", "Per cent"],
        ["Mnemonic", "FZCF0D", "FZCY0D"],
        ["2026-01-01", 3.5, 3.4],
    ]

    rows = _metadata_from_header_rows(
        sheet_rows,
        table_id="zcr-analytical",
        sheet_name="Forward rates",
        source="rba_xlsx_hist",
        category="Interest Rates and Yields",
    )
    assert len(rows) == 2
    assert {r["series_id"] for r in rows} == {"FZCF0D", "FZCY0D"}
    assert all(r["source"] == "rba_xlsx_hist" for r in rows)
    assert all(r["table_id"] == "zcr-analytical/Forward rates" for r in rows)


def test_parse_xlsx_exclusive_skips_sheets_outside_allowlist() -> None:
    """``_parse_xlsx_exclusive`` must only emit rows from the explicit
    allow-list. This guards against accidentally double-counting series
    already captured via the CSV index when RBA republishes a sheet
    under both formats.
    """
    from parsimony_rba import _parse_xlsx_exclusive

    xlsx_bytes = _make_xlsx_fixture(
        [
            (
                "In Allow List",
                _rba_metadata_rows("Keep me", "Allow-listed sheet", "Per cent", "Daily", "KEEP1", 1.0),
            ),
            (
                "Not In Allow List",
                _rba_metadata_rows("Skip me", "Not allow-listed", "Per cent", "Daily", "SKIP1", 1.0),
            ),
        ]
    )

    rows = _parse_xlsx_exclusive(xlsx_bytes, table_id="example", allowed_sheets=("In Allow List",))
    ids = {r["series_id"] for r in rows}
    assert ids == {"KEEP1"}
    assert all(r["source"] == "rba_xlsx" for r in rows)


def test_parse_xlsx_exclusive_returns_empty_on_invalid_bytes() -> None:
    """Malformed XLSX payloads must not crash the enumerator. The parser
    returns an empty list so the orchestrator's try/except is belt-and-
    braces; this keeps a single bad workbook from torpedoing the run.
    """
    from parsimony_rba import _parse_xlsx_exclusive

    rows = _parse_xlsx_exclusive(b"not-really-xlsx", table_id="bad", allowed_sheets=("Data",))
    assert rows == []
