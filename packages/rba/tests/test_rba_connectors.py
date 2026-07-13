"""Offline tests for the RBA connectors.

RBA is Akamai-protected, so the live transport is **curl_cffi** (browser
impersonation) — plain httpx is TLS-fingerprint-blocked (403). These offline
tests therefore mock curl_cffi (not httpx/respx): a fake ``Session.get``
serves canned RBA payloads keyed by URL. The ``_FakeSession`` fixture below is
the shared mock harness.
"""

from __future__ import annotations

from typing import Any

import pytest
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    ProviderError,
    RateLimitError,
)

import parsimony_rba as pkg
import parsimony_rba._http as rba_http
import parsimony_rba.connectors.enumerate as rba_enum
import parsimony_rba.outputs as rba_outputs
import parsimony_rba.parsing as rba_parsing
from parsimony_rba import (
    CONNECTORS,
    enumerate_rba,
    rba_fetch,
)

# ---------------------------------------------------------------------------
# curl_cffi mock harness
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for a curl_cffi Response.

    Carries ``status_code``, ``text``, ``content`` (bytes), and a ``headers``
    mapping with a ``.get`` — enough for ``_curl_get`` and ``parse_retry_after``.
    """

    def __init__(
        self,
        status_code: int = 200,
        *,
        text: str = "",
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")
        self.headers = headers or {}


class _FakeSession:
    """A fake curl_cffi ``Session`` driven by a ``{url: response}`` map.

    A value may also be an Exception instance/class — it is raised on GET to
    exercise the transport-error mapping path. A missing URL yields a 404.
    """

    def __init__(self, routes: dict[str, Any]) -> None:
        self._routes = routes
        self.calls: list[str] = []

    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def get(self, url: str, *, impersonate: str = "chrome", timeout: float = 60.0) -> _FakeResponse:
        self.calls.append(url)
        value = self._routes.get(url)
        if value is None:
            return _FakeResponse(404, text="not found")
        if isinstance(value, BaseException):
            raise value
        if isinstance(value, type) and issubclass(value, BaseException):
            raise value("boom")
        assert isinstance(value, _FakeResponse)
        return value


def _install_session(monkeypatch: pytest.MonkeyPatch, routes: dict[str, Any]) -> _FakeSession:
    """Patch ``_make_session`` (in ``parsimony_rba._http``, where the connectors look
    it up) to return a single shared ``_FakeSession``."""
    session = _FakeSession(routes)
    monkeypatch.setattr(rba_http, "_make_session", lambda: session)
    return session


# ---------------------------------------------------------------------------
# Fixtures — canned RBA payloads
# ---------------------------------------------------------------------------

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

_TABLES_URL = "https://www.rba.gov.au/statistics/tables/"
_HIST_URL = "https://www.rba.gov.au/statistics/historical-data.html"


def _csv_url(stem: str) -> str:
    return f"https://www.rba.gov.au/statistics/tables/csv/{stem}.csv"


# ---------------------------------------------------------------------------
# Collection shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"rba_fetch", "enumerate_rba", "rba_search"}


def test_no_dead_params_models_remain() -> None:
    """The pydantic ``*Params`` models were deleted in the 0.7 sweep — the
    connector inline-validates instead. Guard against a regression that
    reintroduces a bundled request object."""
    assert not hasattr(pkg, "RbaFetchParams")
    assert not hasattr(pkg, "RbaEnumerateParams")


# ---------------------------------------------------------------------------
# rba_fetch — happy path + validation
# ---------------------------------------------------------------------------


def test_rba_fetch_resolves_then_parses_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=_F1_CSV),
        },
    )

    result = rba_fetch(table_id="f1-data")

    assert result.provenance.source == "rba_fetch"
    assert result.provenance.params == {"table_id": "f1-data"}
    df = result.raw
    assert "table_id" in df.columns
    assert df.iloc[0]["table_id"] == "f1-data"
    assert df.iloc[0]["series_key"] == "FIRMMCRTD"
    assert df["value"].notna().any()


def test_rba_fetch_normalises_trailing_csv_suffix_and_case(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inline validation lowercases and strips a stray ``.csv`` suffix
    (replaces the deleted RbaFetchParams normaliser)."""
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=_F1_CSV),
        },
    )

    # Mixed case + a stray .csv suffix still resolves to f1-data and parses.
    # (Provenance records the verbatim call-time arg; the normalisation is
    # internal and surfaces in the resolved table_id stamped on the data.)
    result = rba_fetch(table_id="F1-DATA.csv")
    df = result.raw
    assert set(df["table_id"]) == {"f1-data"}
    assert "FIRMMCRTD" in set(df["series_key"])


def test_rba_fetch_rejects_blank_table_id() -> None:
    with pytest.raises(InvalidParameterError):
        rba_fetch(table_id="   ")


def test_rba_fetch_raises_invalid_parameter_for_unknown_table(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_session(monkeypatch, {_TABLES_URL: _FakeResponse(200, text=_TABLES_HTML)})

    with pytest.raises(InvalidParameterError, match="not found"):
        rba_fetch(table_id="nonexistent-table")


def test_rba_fetch_raises_empty_data_on_dataless_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 200 CSV whose data section has no rows → EmptyDataError (with the
    query params that produced it, per §5)."""
    dataless = (
        "F1 INTEREST RATES\n"
        ",Cash Rate Target\n"
        "Title,Cash Rate Target\n"
        "Description,Official cash rate target\n"
        "Frequency,Daily\n"
        "Units,Per cent\n"
        "Series ID,FIRMMCRTD\n"
    )
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=dataless),
        },
    )

    with pytest.raises(EmptyDataError) as exc_info:
        rba_fetch(table_id="f1-data")
    assert exc_info.value.query_params == {"table_id": "f1-data"}


# ---------------------------------------------------------------------------
# enumerate_rba — catalog completeness
# ---------------------------------------------------------------------------


def test_enumerate_rba_emits_description_table_id_unit_and_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One row per Series ID with the full Treasury-grade column set.

    * compound ``code`` for KEY,
    * ``description`` populated from the CSV's Description row,
    * ``table_id`` and ``series_id`` exposed as METADATA,
    * ``unit`` captured from the Units row,
    * ``source`` set to ``"rba_csv"`` for dispatch consistency.
    """
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=_F1_CSV),
            _csv_url("g1-data"): _FakeResponse(404, text="missing"),
            _HIST_URL: _FakeResponse(200, text="<html></html>"),
        },
    )

    df = (enumerate_rba()).raw

    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == list(rba_outputs._ENUMERATE_COLUMNS)

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


def test_enumerate_rba_compound_code_keeps_cross_table_series_id_collisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Series ids reused across tables (B13.1.x vs B13.2.x in the wild) must
    emit two distinct catalog rows, distinguished by ``table_id`` in the
    compound ``code``. A bare ``series_id`` KEY would silently drop the
    second occurrence — the compound code is what keeps both reachable."""
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML_COLLISION),
            _csv_url("b13-1-2-africa"): _FakeResponse(200, text=_B13_1_2_CSV),
            _csv_url("b13-2-1-africa"): _FakeResponse(200, text=_B13_2_1_CSV),
            _HIST_URL: _FakeResponse(200, text="<html></html>"),
        },
    )

    df = (enumerate_rba()).raw

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


def test_enumerate_rba_source_metadata_uniform(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every emitted row carries ``source='rba_csv'`` so an agent dispatching
    off a search hit knows which fetch connector to call without parsing
    the code prefix."""
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML_COLLISION),
            _csv_url("b13-1-2-africa"): _FakeResponse(200, text=_B13_1_2_CSV),
            _csv_url("b13-2-1-africa"): _FakeResponse(200, text=_B13_2_1_CSV),
            _HIST_URL: _FakeResponse(200, text="<html></html>"),
        },
    )

    df = (enumerate_rba()).raw
    assert set(df["source"]) == {"rba_csv"}


def test_enumerate_rba_swallows_per_csv_fetch_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """A single bad CSV (500) must not torpedo the whole crawl — the enumerator
    swallows per-fetch errors and keeps the surviving rows."""
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=_F1_CSV),
            _csv_url("g1-data"): _FakeResponse(500, text="boom"),
            _HIST_URL: _FakeResponse(200, text="<html></html>"),
        },
    )

    df = (enumerate_rba()).raw
    assert "FIRMMCRTD" in set(df["series_id"])


def test_enumerate_rba_bounding_seam_limits_fan_out(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_discover_csv_links`` is the bounding seam — monkeypatching it to a
    single link reduces the CSV fan-out to one request (the live-test bound)."""

    def _one_link(_session: Any) -> list[str]:
        return ["/statistics/tables/csv/f1-data.csv"]

    monkeypatch.setattr(rba_enum, "_discover_csv_links", _one_link)
    session = _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=_F1_CSV),
            _HIST_URL: _FakeResponse(200, text="<html></html>"),
        },
    )

    df = (enumerate_rba()).raw
    # Only f1-data was fetched (plus the index + historical-data page) — the
    # g1-data link advertised by the index was never requested.
    assert _csv_url("g1-data") not in session.calls
    assert "FIRMMCRTD" in set(df["series_id"])


# ---------------------------------------------------------------------------
# XLSX-exclusive sheet + xls-hist coverage
# ---------------------------------------------------------------------------


def _make_xlsx_fixture(sheets: list[tuple[str, list[list[object]]]]) -> bytes:
    """Build an in-memory XLSX workbook with the given sheets and return its bytes."""
    import io

    import openpyxl

    wb = openpyxl.Workbook()
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
    Frequency / Units / Series ID) plus a single data row."""
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


def test_enumerate_rba_xlsx_dynamic_exclusivity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dynamic XLSX exclusivity: a current workbook's series are emitted ONLY when
    not already covered by the CSV pass (no hardcoded sheet allow-list).

    ``a03.xlsx``'s Bond Purchase Program sheet holds a series (``ALDBPPFVD``) never
    republished as a CSV → it must be emitted tagged ``source='rba_xlsx'``. The
    ``ES Balances`` sheet's series (``CSVDUP1``) *is* in the CSV pass, so the XLSX
    pass must skip it (no duplicate row).
    """
    xlsx_data = _make_xlsx_fixture(
        [
            (
                "ES Balances and Repo Agreements",
                _rba_metadata_rows(
                    title="ES Balances",
                    description="Also published as a CSV",
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

    # a1-data CSV declares CSVDUP1 — so the XLSX "ES Balances" sheet is a duplicate.
    a1_csv = (
        "A1 RESERVE BANK\n"
        ",ES Balances\n"
        "Title,ES Balances\n"
        "Description,Exchange settlement balances\n"
        "Frequency,Daily\n"
        "Units,$m\n"
        "Series ID,CSVDUP1\n"
        "01-Jan-2026,100\n"
        "02-Jan-2026,101\n"
    )

    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML_WITH_XLSX),
            _csv_url("a1-data"): _FakeResponse(200, text=a1_csv),
            "https://www.rba.gov.au/statistics/tables/xls/a03.xlsx": _FakeResponse(200, content=xlsx_data),
            _HIST_URL: _FakeResponse(200, text="<html></html>"),
        },
    )

    df = (enumerate_rba()).raw

    # The XLSX-exclusive Bond Purchase Program series is emitted (not in any CSV).
    bpp = df[df["series_id"] == "ALDBPPFVD"]
    assert len(bpp) == 1, "Bond Purchase Program series must be emitted exactly once"
    row = bpp.iloc[0]
    assert row["source"] == "rba_xlsx"
    assert row["title"] == "Face Value"
    assert row["unit"] == "$ million"
    assert row["table_id"] == "a03/Bond Purchase Program"
    assert row["code"] == "a03/Bond Purchase Program#ALDBPPFVD"

    # CSVDUP1 is covered by the CSV pass → the XLSX pass must NOT emit a duplicate.
    dup = df[df["series_id"] == "CSVDUP1"]
    assert len(dup) == 1, "covered series must appear once (from CSV, not duplicated by XLSX)"
    assert dup.iloc[0]["source"] == "rba_csv"


def test_enumerate_rba_pulls_xls_hist_discontinued_series(monkeypatch: pytest.MonkeyPatch) -> None:
    """``xls-hist/*.xls`` legacy binaries expose discontinued series that
    dropped out of the live CSVs. They're catalogued with
    ``source='rba_xlsx_hist'``. We mock the inner parse function — the test
    exercises the ENUMERATOR's orchestration (discover link, fetch, merge)."""
    historical_html = """
    <html><body>
    <a href="/statistics/tables/xls-hist/b03hist.xls">B3 Repo Agreements (historical)</a>
    <a href="/statistics/tables/xls-hist/1983-1986.xls">Period archive (no series IDs)</a>
    </body></html>
    """

    def fake_parse(data: bytes, table_id: str) -> list[dict[str, str]]:
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

    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text=_F1_CSV),
            _csv_url("g1-data"): _FakeResponse(404, text="missing"),
            _HIST_URL: _FakeResponse(200, text=historical_html),
            "https://www.rba.gov.au/statistics/tables/xls-hist/b03hist.xls": _FakeResponse(
                200, content=b"fake-xls-bytes"
            ),
        },
    )

    monkeypatch.setattr(rba_parsing, "_parse_xls_hist", fake_parse)
    df = (enumerate_rba()).raw

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
    parser must treat both tokens identically."""
    from parsimony_rba.parsing import _metadata_from_header_rows

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


def test_parse_xlsx_workbook_exclusive_emits_only_uncovered_series() -> None:
    """``_parse_xlsx_workbook_exclusive`` emits only series whose id is NOT already in
    the covered (CSV-derived) set — dynamic exclusivity, no hardcoded allow-list."""
    from parsimony_rba.parsing import _parse_xlsx_workbook_exclusive

    xlsx_bytes = _make_xlsx_fixture(
        [
            (
                "Sheet A",
                _rba_metadata_rows("Covered", "Already in a CSV", "Per cent", "Daily", "COVERED1", 1.0),
            ),
            (
                "Sheet B",
                _rba_metadata_rows("Exclusive", "XLSX-only series", "Per cent", "Daily", "EXCL1", 1.0),
            ),
            ("Notes", [["Notes"], ["Some notes text"]]),
        ]
    )

    rows = _parse_xlsx_workbook_exclusive(xlsx_bytes, "example", covered_ids={"COVERED1"})
    ids = {r["series_id"] for r in rows}
    assert ids == {"EXCL1"}
    assert all(r["source"] == "rba_xlsx" for r in rows)
    assert all(r["table_id"] == "example/Sheet B" for r in rows)


def test_parse_xlsx_workbook_exclusive_returns_empty_on_invalid_bytes() -> None:
    """Malformed XLSX payloads must not crash the enumerator."""
    from parsimony_rba.parsing import _parse_xlsx_workbook_exclusive

    rows = _parse_xlsx_workbook_exclusive(b"not-really-xlsx", "bad", covered_ids=set())
    assert rows == []


def test_melt_sheet_rows_handles_mnemonic_and_datetime_cells() -> None:
    """``_melt_sheet_rows`` (the workbook-fetch melter) handles a ``Mnemonic`` id row,
    datetime period cells (xlsx/xls), string period cells, and missing values."""
    from datetime import datetime

    from parsimony_rba.parsing import _melt_sheet_rows

    rows: list[list[object]] = [
        ["TABLE HEADING", "", ""],
        ["Title", "Series One", "Series Two"],
        ["Mnemonic", "AAA", "BBB"],
        [datetime(2020, 1, 1), 1.5, 2.5],
        ["2020-02-01", 3.0, None],
    ]
    df = _melt_sheet_rows(rows, "x/Sheet")
    assert set(df["table_id"]) == {"x/Sheet"}
    assert set(df["series_key"]) == {"AAA", "BBB"}
    aaa = df[df["series_key"] == "AAA"].sort_values("date")
    assert aaa["value"].tolist() == [1.5, 3.0]
    assert "2020-01-01" in set(df["date"]), "datetime period cell not normalized"


def test_rba_fetch_xlsx_exclusive_sheet(monkeypatch: pytest.MonkeyPatch) -> None:
    """``rba_fetch`` resolves a workbook+sheet table_id (``a03/Bond Purchase Program``)
    to the XLSX host and melts that sheet's data — the closed CSV-only gap. No tables
    index scrape is needed: the workbook URL is constructed directly from the stem."""
    sheet_rows: list[list[object]] = [
        ["BOND PURCHASE PROGRAM", "", ""],
        ["Title", "Face Value", "Coupon"],
        ["Description", "Face value of bonds", "Coupon rate"],
        ["Frequency", "As required", "As required"],
        ["Units", "$ million", "Per cent"],
        ["Series ID", "ALDBPPFVD", "ALDBPPCP"],
        ["2021-01-15", 500.0, 1.25],
        ["2021-02-15", 750.0, 1.10],
    ]
    xlsx_data = _make_xlsx_fixture([("Bond Purchase Program", sheet_rows)])
    _install_session(
        monkeypatch,
        {"https://www.rba.gov.au/statistics/tables/xls/a03.xlsx": _FakeResponse(200, content=xlsx_data)},
    )

    result = rba_fetch(table_id="a03/Bond Purchase Program")

    assert result.provenance.source == "rba_fetch"
    df = result.raw
    assert set(df["table_id"]) == {"a03/Bond Purchase Program"}
    assert set(df["series_key"]) == {"ALDBPPFVD", "ALDBPPCP"}
    fv = df[df["series_key"] == "ALDBPPFVD"].sort_values("date")
    assert fv["value"].tolist() == [500.0, 750.0]
    assert df["date"].dtype.kind == "M"


# ---------------------------------------------------------------------------
# rba_fetch — ParseError on a wrong-shape body
# ---------------------------------------------------------------------------


def test_rba_fetch_raises_parse_error_on_unparseable_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the CSV parser raises on a malformed/garbage 200 body, the connector
    surfaces it as ParseError (§5.8), never a crash or a fake status."""

    def _boom(text: str, table_id: str) -> Any:
        raise ValueError("totally unparseable workbook")

    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(200, text="<html>garbage</html>"),
        },
    )
    monkeypatch.setattr(rba_parsing, "_parse_rba_csv", _boom)

    with pytest.raises(ParseError):
        rba_fetch(table_id="f1-data")


# ---------------------------------------------------------------------------
# Smoke: ProviderError / RateLimitError surface from the fetch path. (Full
# status-table coverage lives in test_error_mapping_rba.py.)
# ---------------------------------------------------------------------------


def test_rba_fetch_maps_csv_500_to_provider_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(503, text="unavailable"),
        },
    )
    with pytest.raises(ProviderError) as exc_info:
        rba_fetch(table_id="f1-data")
    assert exc_info.value.status_code == 503


def test_rba_fetch_maps_csv_429_to_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_session(
        monkeypatch,
        {
            _TABLES_URL: _FakeResponse(200, text=_TABLES_HTML),
            _csv_url("f1-data"): _FakeResponse(429, text="slow", headers={"Retry-After": "17"}),
        },
    )
    with pytest.raises(RateLimitError) as exc_info:
        rba_fetch(table_id="f1-data")
    assert exc_info.value.retry_after == 17.0
