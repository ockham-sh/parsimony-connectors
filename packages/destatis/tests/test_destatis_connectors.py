"""Offline (respx-mocked) tests for the Destatis connectors.

Targets the public ``genesis.destatis.de/genesis/api/rest/*`` endpoints
(anonymous, keyless):

* ``destatis_fetch`` — JSON-stat 2.0 over ``/tables/{code}/data``
* ``enumerate_destatis`` — composes ``/statistics``,
  ``/statistics/{code}/information``, and ``/statistics/{code}/tables``
* ``destatis_search`` — semantic search over the published catalog (lazy
  ``Catalog.load`` keeps import-time cheap, so it registers without any network)

GENESIS-Online is anonymous — there are no credentials and nothing that could
leak into provenance.
"""

from __future__ import annotations

import logging

import httpx
import pandas as pd
import pytest
import respx
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    ParseError,
    ProviderError,
    RateLimitError,
)
from parsimony.result import ColumnRole, Result

from parsimony_destatis import CONNECTORS
from parsimony_destatis.connectors.enumerate import enumerate_destatis
from parsimony_destatis.connectors.fetch import destatis_fetch
from parsimony_destatis.outputs import DESTATIS_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

_BASE = "https://genesis.destatis.de/genesis/api/rest"


# A minimal JSON-stat 2.0 payload representing 2 monthly observations of a
# single dimension table — wrapped in the GENESIS-Online ``{"data": [...]}``
# envelope that the live API uses. The inner dataset omits ``label`` (the
# real API does too) so we exercise the title-fallback branch.
_JSONSTAT_FIXTURE = {
    "data": [
        {
            "code": "DS_001",
            "id": ["Zeit"],
            "size": [2],
            "value": [108.4, 108.7],
            "dimension": {
                "Zeit": {
                    "category": {
                        "index": {"2026-01": 0, "2026-02": 1},
                        "label": {
                            "2026-01": "Januar 2026",
                            "2026-02": "Februar 2026",
                        },
                    }
                }
            },
        }
    ]
}


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    """Three connectors ship with the package: fetch, enumerate, and the
    semantic-search tool that maps natural-language queries to codes.
    ``Catalog.load`` is lazy (only invoked on first ``destatis_search``
    call), so import-time registration succeeds without any network or HF
    access.
    """
    names = {c.name for c in CONNECTORS}
    assert names == {"destatis_fetch", "enumerate_destatis", "destatis_search"}


def test_enumerate_output_spec_includes_description_metadata() -> None:
    """``description`` is ordinary metadata in the clean catalog contract."""
    by_name = {c.name: c for c in DESTATIS_ENUMERATE_OUTPUT.columns}
    assert by_name["description"].role == ColumnRole.METADATA
    assert by_name["source"].role == ColumnRole.METADATA
    assert by_name["entity_type"].role == ColumnRole.METADATA
    assert by_name["parent_statistic"].role == ColumnRole.METADATA
    assert by_name["title_de"].role == ColumnRole.METADATA
    assert by_name["title_en"].role == ColumnRole.METADATA


# ---------------------------------------------------------------------------
# destatis_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_destatis_fetch_parses_jsonstat_response() -> None:
    """Happy-path: 2-cell JSON-stat dataset → 2-row long DataFrame with
    parsed German-month dates and float values.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(return_value=httpx.Response(200, json=_JSONSTAT_FIXTURE))

    result = destatis_fetch(name="61111-0001")

    assert result.provenance.source == "destatis_fetch"
    # No credentials → provenance carries only the call args (no secrets).
    assert result.provenance.params["name"] == "61111-0001"
    df = result.data
    assert len(df) == 2
    assert "series_id" in df.columns
    assert df.iloc[0]["series_id"] == "61111-0001"
    # The new GENESIS-Online API doesn't expose a top-level dataset label,
    # so the parser falls back to the table code.
    assert df.iloc[0]["title"] == "61111-0001"
    # Dates parsed via ``_normalize_german_date`` (German month names).
    assert set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")) == {"2026-01-01", "2026-02-01"}
    assert sorted(df["value"].tolist()) == [108.4, 108.7]


@respx.mock
def test_destatis_fetch_forwards_year_range_params() -> None:
    """``start_year`` / ``end_year`` are forwarded as ``startyear`` / ``endyear``
    query params (and ``None`` values are dropped).
    """
    route = respx.get(f"{_BASE}/tables/61111-0001/data").mock(return_value=httpx.Response(200, json=_JSONSTAT_FIXTURE))

    destatis_fetch(name="61111-0001", start_year="2020")

    request = route.calls.last.request
    assert request.url.params.get("startyear") == "2020"
    assert "endyear" not in request.url.params


#: JSON-stat fixture spanning two years (2025-01 and 2026-01), used to prove the
#: client-side year filter drops rows GENESIS returned outside the window.
_JSONSTAT_TWO_YEARS = {
    "data": [
        {
            "code": "DS_002",
            "id": ["Zeit"],
            "size": [2],
            "value": [100.0, 200.0],
            "dimension": {
                "Zeit": {"category": {"index": {"2025-01": 0, "2026-01": 1}}}
            },
        }
    ]
}


@respx.mock
def test_destatis_fetch_applies_client_side_year_filter() -> None:
    """GENESIS ignores ``startyear`` on some tables and returns the full span, so
    the connector re-applies the window client-side: a 2025+2026 payload narrowed
    to ``start_year=2026`` keeps only the 2026 row.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(200, json=_JSONSTAT_TWO_YEARS)
    )

    df = destatis_fetch(name="61111-0001", start_year="2026").data

    assert len(df) == 1
    assert pd.to_datetime(df["date"]).dt.year.tolist() == [2026]
    assert df.iloc[0]["value"] == 200.0


@respx.mock
def test_destatis_fetch_empty_window_raises_empty_data() -> None:
    """A window that filters every row away raises ``EmptyDataError`` (matching
    the existing no-observations path) rather than returning an empty frame.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(200, json=_JSONSTAT_TWO_YEARS)
    )

    with pytest.raises(EmptyDataError):
        destatis_fetch(name="61111-0001", start_year="2030")


@respx.mock
def test_destatis_fetch_maps_404_to_provider_error() -> None:
    """An unknown table code returns a real HTTP 404 (verified live) → mapped
    to ``ProviderError(404)`` by ``check_status``.
    """
    respx.get(f"{_BASE}/tables/ZZZZZ-9999/data").mock(return_value=httpx.Response(404))

    with pytest.raises(ProviderError) as exc_info:
        destatis_fetch(name="ZZZZZ-9999")
    assert exc_info.value.status_code == 404


@respx.mock
def test_destatis_fetch_maps_500_to_provider_error() -> None:
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(return_value=httpx.Response(500, text="upstream error"))

    with pytest.raises(ProviderError) as exc_info:
        destatis_fetch(name="61111-0001")
    assert exc_info.value.status_code == 500


@respx.mock
def test_destatis_fetch_html_announcement_maps_to_parse_error() -> None:
    """§5.8 — a 200 with the SPA / maintenance HTML shell is a ParseError
    (200 but not the data shape we expected), NOT a fake ``status_code=0``.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(200, text="<!doctype html><html><body>Wartungsarbeiten</body></html>")
    )

    with pytest.raises(ParseError, match="HTML"):
        destatis_fetch(name="61111-0001")


@respx.mock
def test_destatis_fetch_throttle_html_maps_to_rate_limit() -> None:
    """§5.8 — a 200 HTML body carrying a quota/throttle phrase maps to
    ``RateLimitError(quota_exhausted=True)`` rather than ParseError.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(
            200,
            text="<html><body>Zu viele Anfragen – Kontingent ausgeschöpft.</body></html>",
        )
    )

    with pytest.raises(RateLimitError) as exc_info:
        destatis_fetch(name="61111-0001")
    assert exc_info.value.quota_exhausted is True


@respx.mock
def test_destatis_fetch_non_dataset_json_maps_to_parse_error() -> None:
    """A 200 JSON body that is not a JSON-stat dataset/envelope → ParseError."""
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(return_value=httpx.Response(200, json={"unexpected": "shape"}))

    with pytest.raises(ParseError, match="envelope"):
        destatis_fetch(name="61111-0001")


@respx.mock
def test_destatis_fetch_all_null_values_maps_to_empty_data() -> None:
    """A dataset whose every cell is null parses to zero rows → EmptyDataError
    carrying the query params for parameter adjustment.
    """
    fixture = {
        "data": [
            {
                "code": "DS_001",
                "id": ["Zeit"],
                "size": [2],
                "value": [None, None],
                "dimension": {"Zeit": {"category": {"index": {"2026-01": 0, "2026-02": 1}}}},
            }
        ]
    }
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(return_value=httpx.Response(200, json=fixture))

    with pytest.raises(EmptyDataError) as exc_info:
        destatis_fetch(name="61111-0001")
    assert exc_info.value.query_params.get("name") == "61111-0001"


# ---------------------------------------------------------------------------
# Time-dimension detection (the headline live fix)
#
# GENESIS marks the time axis as the dimension whose category-index KEYS are the
# period values; ``role`` is absent and ``label`` is null. The first dim is
# always the constant ``statistic`` dim. The detector must pick the time dim by
# key shape, never by name (which misses STAG/SEMEST/SMONAT and false-matches
# the MONAT/QUARTG month-/quarter-of-year classifications) and never fall back
# to dim 0 (which emitted the statistic code as a bogus "year").
# ---------------------------------------------------------------------------


def _jsonstat(table_code: str, ids: list[str], index: dict[str, list[str]], value: list) -> dict:
    """Build a minimal JSON-stat 2.0 ``{data:[ds]}`` envelope from ordered keys."""
    return {
        "data": [
            {
                "code": table_code,
                "id": ids,
                "size": [len(index[d]) for d in ids],
                "value": value,
                "dimension": {
                    d: {"category": {"index": {k: i for i, k in enumerate(index[d])}}} for d in ids
                },
            }
        ]
    }


@respx.mock
def test_fetch_time_dim_is_reference_date_not_statistic_code() -> None:
    """A reference-date (``STAG``) table: dims ``[statistic, STAG]``. The old
    name-based detector fell back to dim 0 (``statistic``) and emitted the
    statistic code ``12411`` as a year → ParseError. The key-shape detector
    must pick ``STAG`` and parse the ISO dates.
    """
    fixture = _jsonstat(
        "12411-0001",
        ["statistic", "STAG"],
        {"statistic": ["12411"], "STAG": ["1999-12-31", "2009-12-31", "2019-12-31"]},
        [82.0, 81.8, 83.2],
    )
    respx.get(f"{_BASE}/tables/12411-0001/data").mock(return_value=httpx.Response(200, json=fixture))

    df = (destatis_fetch(name="12411-0001")).data
    assert df["date"].dtype.kind == "M"
    assert set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")) == {
        "1999-12-31",
        "2009-12-31",
        "2019-12-31",
    }
    # The statistic code never leaks into the date axis; it rides as its own col.
    assert (df["statistic"] == "12411").all()


@respx.mock
def test_fetch_time_dim_iso_duration_period() -> None:
    """A monthly (``SMONAT``) table keys time as ISO-8601 durations
    (``2015-05P1M``); the period is normalised to its start month.
    """
    fixture = _jsonstat(
        "42153-0001",
        ["statistic", "SMONAT"],
        {"statistic": ["42153"], "SMONAT": ["2015-05P1M", "2015-06P1M"]},
        [100.1, 100.4],
    )
    respx.get(f"{_BASE}/tables/42153-0001/data").mock(return_value=httpx.Response(200, json=fixture))

    df = (destatis_fetch(name="42153-0001")).data
    assert set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")) == {"2015-05-01", "2015-06-01"}


@respx.mock
def test_fetch_month_of_year_classification_is_not_treated_as_time() -> None:
    """A table with BOTH a ``MONAT`` month-of-year *classification*
    (keys ``MONAT01``…) and a real ``JAHR`` time axis: the detector must pick
    ``JAHR`` (period-shaped keys) for the date and keep ``MONAT`` as an ordinary
    classification column — never the reverse.
    """
    fixture = _jsonstat(
        "00000-0001",
        ["statistic", "MONAT", "JAHR"],
        {"statistic": ["00000"], "MONAT": ["MONAT01", "MONAT02"], "JAHR": ["2023", "2024"]},
        # row-major over sizes [1,2,2]: (m0,j0),(m0,j1),(m1,j0),(m1,j1)
        [1.0, 2.0, 3.0, 4.0],
    )
    respx.get(f"{_BASE}/tables/00000-0001/data").mock(return_value=httpx.Response(200, json=fixture))

    df = (destatis_fetch(name="00000-0001")).data
    # Dates are the real years, not the MONAT codes or the statistic code.
    assert set(pd.to_datetime(df["date"]).dt.year) == {2023, 2024}
    # MONAT survives as a classification column carrying its month-of-year codes.
    assert "MONAT" in df.columns
    assert set(df["MONAT"]) == {"MONAT01", "MONAT02"}


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_fetch_rejects_empty_table_code() -> None:
    """``name=`` (the canonical field) must be non-empty — validated inline,
    before any network call, as ``InvalidParameterError`` (no params model).
    """
    with pytest.raises(InvalidParameterError):
        destatis_fetch(name="   ")


# ---------------------------------------------------------------------------
# enumerate_destatis
# ---------------------------------------------------------------------------


def _stub_index(statistics: list[dict]) -> respx.Route:
    # The live ``/statistics`` index returns a bare JSON list.
    return respx.get(f"{_BASE}/statistics").mock(return_value=httpx.Response(200, json=statistics))


def _stub_information(code: str, *, status: int = 200, payload: dict | None = None) -> respx.Route:
    return respx.get(f"{_BASE}/statistics/{code}/information").mock(
        return_value=httpx.Response(status, json=payload or {})
    )


def _stub_tables(code: str, *, status: int = 200, tables: list[dict] | None = None) -> respx.Route:
    # The live ``/tables`` endpoint returns a bare JSON list.
    return respx.get(f"{_BASE}/statistics/{code}/tables").mock(return_value=httpx.Response(status, json=tables or []))


# Real GENESIS shapes: statistic nodes carry ``statisticalCategoryNames`` and
# ``variableCodes``/``variableNames``; table nodes carry ``code``/``name`` +
# ``variableCodes``/``variableNames``. (Verified live.)
_STAT_61111 = {
    "code": "61111",
    "name": {"de": "Verbraucherpreisindex", "en": "Consumer price index"},
    "statisticalCategoryNames": [
        {"de": "Preise", "en": "Prices"},
        {"de": "Verbraucherpreise", "en": "Consumer prices"},
    ],
    "variableCodes": ["PREIS1"],
    "variableNames": [{"de": "Verbraucherpreisindex", "en": "Consumer price index"}],
}


@respx.mock
def test_enumerate_destatis_emits_statistic_and_table_rows() -> None:
    """Round-trip through the three-call composition: ``/statistics`` ⇒
    one statistic row + one row per table from
    ``/statistics/{code}/tables``. Both rows must carry a non-empty description,
    and the metadata columns must be populated from the REAL payload shape.
    """
    _stub_index([_STAT_61111])
    _stub_information(
        "61111",
        payload={
            "code": "61111",
            "name": {"de": "Verbraucherpreisindex", "en": "Consumer price index"},
            "description": {
                "de": (
                    "Der Verbraucherpreisindex (VPI) misst die durchschnittliche "
                    "Preisentwicklung aller Waren und Dienstleistungen, die von "
                    "privaten Haushalten zu Konsumzwecken gekauft werden."
                ),
            },
        },
    )
    _stub_tables(
        "61111",
        tables=[
            {
                "code": "61111-0001",
                "name": {"de": "VPI: Deutschland, Monate", "en": "CPI: Germany, monthly"},
                "variableCodes": ["PREIS1", "ZEIT"],
                "variableNames": [
                    {"de": "Index", "en": "Index"},
                    {"de": "Zeit", "en": "Time"},
                ],
            }
        ],
    )

    result = enumerate_destatis()
    df = result.data

    # Exactly the 11-column schema, in declared order.
    assert list(df.columns) == list(ENUMERATE_COLUMNS)

    statistic_rows = df[df["entity_type"] == "statistic"]
    table_rows = df[df["entity_type"] == "table"]
    assert len(statistic_rows) == 1
    assert len(table_rows) == 1

    stat = statistic_rows.iloc[0]
    assert stat["code"] == "61111"
    assert stat["title"] == "Consumer price index"  # English preferred
    assert stat["title_de"] == "Verbraucherpreisindex"
    assert stat["title_en"] == "Consumer price index"
    assert stat["description"]  # non-empty
    assert "Verbraucherpreisindex" in stat["description"]
    assert stat["source"] == "genesis_online"
    # subject_area derived from statisticalCategoryNames (NOT the absent
    # ``subjectArea`` field) — the most-specific category, English.
    assert stat["subject_area"] == "Consumer prices"
    # Statistic-level variables surfaced (was always-empty before the fix).
    assert stat["variable_codes"] == "PREIS1"
    assert stat["variable_names_en"] == "Consumer price index"

    table = table_rows.iloc[0]
    assert table["code"] == "61111-0001"
    assert table["title"] == "CPI: Germany, monthly"
    assert table["parent_statistic"] == "61111"
    # Table variables read from variableCodes/variableNames (the real shape).
    assert table["variable_codes"] == "PREIS1,ZEIT"
    assert table["variable_names_en"] == "Index,Time"
    assert table["source"] == "genesis_online"


@respx.mock
def test_enumerate_destatis_lifts_parent_description_into_table_rows() -> None:
    """Per-table semantic queries need the long parent description as
    retrieval signal — table titles alone are too thin for the embedder.
    """
    parent_de = (
        "Der Verbraucherpreisindex misst die durchschnittliche Preisentwicklung "
        "aller Waren und Dienstleistungen für private Haushalte. Der Index ist "
        "ein zentraler Indikator für die Inflation."
    )
    _stub_index([{"code": "61111", "name": {"de": "VPI", "en": "CPI"}}])
    _stub_information(
        "61111",
        payload={"description": {"de": parent_de}, "name": {"de": "VPI", "en": "CPI"}},
    )
    _stub_tables(
        "61111",
        tables=[
            {"code": "61111-0001", "name": {"de": "VPI Monate", "en": "CPI monthly"}},
            {"code": "61111-0002", "name": {"de": "VPI Jahre", "en": "CPI annual"}},
        ],
    )

    result = enumerate_destatis()
    df = result.data

    table_rows = df[df["entity_type"] == "table"]
    assert len(table_rows) == 2
    for _, row in table_rows.iterrows():
        assert "Preisentwicklung" in row["description"]
        assert "Parent statistic: CPI (61111)" in row["description"]


@respx.mock
def test_enumerate_destatis_keeps_statistic_when_subresources_fail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per-statistic 429s must not crash enumeration AND must not drop the
    statistic. After exhausting retries the statistic's own row is still
    emitted from the index node (we have its code+name); only the tables/
    description are missing. The operator log notes it as degraded.
    """
    _stub_index([{"code": "61111", "name": {"de": "VPI", "en": "CPI"}}])
    respx.get(f"{_BASE}/statistics/61111/information").mock(return_value=httpx.Response(429))
    respx.get(f"{_BASE}/statistics/61111/tables").mock(return_value=httpx.Response(429))

    with caplog.at_level(logging.INFO, logger="parsimony_destatis"):
        result = enumerate_destatis()

    df = result.data
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    # The statistic survives as a single statistic row (no tables).
    assert len(df) == 1
    assert df.iloc[0]["code"] == "61111"
    assert df.iloc[0]["entity_type"] == "statistic"
    assert any("no tables/description" in r.message for r in caplog.records)


@respx.mock
def test_enumerate_destatis_tableless_statistic_still_emitted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A *tableless* statistic — live shape: ``/information`` is an empty list
    and ``/tables`` 404s (e.g. 61121) — is a legitimate "zero tables", not a
    fetch failure, and must still appear in the catalog as its own statistic
    row. Previously such a statistic vanished entirely.
    """
    _stub_index([{"code": "61121", "name": {"de": "VPI Sonderauswertung", "en": "CPI special"}}])
    respx.get(f"{_BASE}/statistics/61121/information").mock(
        return_value=httpx.Response(200, json=[])
    )
    respx.get(f"{_BASE}/statistics/61121/tables").mock(return_value=httpx.Response(404))

    with caplog.at_level(logging.INFO, logger="parsimony_destatis"):
        result = enumerate_destatis()

    df = result.data
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["code"] == "61121"
    assert row["entity_type"] == "statistic"
    assert row["title"] == "CPI special"
    assert row["source"] == "genesis_online"


@respx.mock
def test_enumerate_destatis_empty_index_emits_header_only_frame(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``/statistics`` index with no usable entries → a header-only frame
    (exact schema, zero rows) and a WARN, never a crash.
    """
    _stub_index([])

    with caplog.at_level(logging.WARNING, logger="parsimony_destatis"):
        result = enumerate_destatis()

    df = result.data
    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert df.empty


@respx.mock
def test_enumerate_destatis_emits_columns_required_for_catalog_entries() -> None:
    """The ``Result`` returned by the enumerator carries catalog entries with
    exactly one KEY (code), one TITLE (title), and METADATA columns.
    """
    _stub_index([{"code": "61111", "name": {"de": "VPI", "en": "CPI"}}])
    _stub_information(
        "61111",
        payload={
            "name": {"de": "VPI", "en": "CPI"},
            "description": {"de": "Der Verbraucherpreisindex misst Preise."},
        },
    )
    _stub_tables(
        "61111",
        tables=[{"code": "61111-0001", "name": {"de": "VPI Monate", "en": "CPI monthly"}}],
    )

    result = enumerate_destatis()
    entries = Result(data=result.data, output_spec=DESTATIS_ENUMERATE_OUTPUT).to_entities()

    by_code = {e.code: e for e in entries}
    assert "61111" in by_code
    assert "61111-0001" in by_code

    stat_entry = by_code["61111"]
    assert stat_entry.namespace == "destatis"
    assert stat_entry.title == "CPI"
    assert stat_entry.metadata.get("description")
    assert stat_entry.metadata.get("entity_type") == "statistic"
    assert stat_entry.metadata.get("source") == "genesis_online"

    table_entry = by_code["61111-0001"]
    assert table_entry.namespace == "destatis"
    assert table_entry.metadata.get("entity_type") == "table"
    assert table_entry.metadata.get("parent_statistic") == "61111"
    assert table_entry.metadata.get("source") == "genesis_online"
