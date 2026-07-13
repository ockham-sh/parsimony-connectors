"""Offline tests for the Bank of Japan connectors.

BoJ is public (no api_key); the template 401/429 credential contract does not
apply. ``boj_fetch`` now uses the canonical ``make_http_client`` +
``fetch_json`` transport and ``enumerate_boj`` keeps the shared
``ThrottledJsonFetcher`` for the Akamai-aware crawl — both run on httpx, so
respx hooks the transport in either case.

Fixtures mirror the REAL BoJ ``getMetadata`` shape verified by live probing:
section header rows have an empty ``SERIES_CODE`` with the section title in
``NAME_OF_TIME_SERIES`` and an integer ``LAYER1`` ORDINAL (``LAYER2..5 == 0``);
series rows carry a real ``SERIES_CODE`` and their parent-section ordinal in
``LAYER1``. The breadcrumb is the section header's name for the series'
``LAYER1`` ordinal — NOT the raw integer layer values.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.catalog.source import entities_from_raw
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import ColumnRole
from parsimony_shared.cb_enumerate import MetadataCrawlConfig

from parsimony_boj import (
    BOJ_ENUMERATE_OUTPUT,
    CONNECTORS,
    boj_fetch,
    enumerate_boj,
)

_DATA_URL = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"
_META_URL = "https://www.stat-search.boj.or.jp/api/v1/getMetadata"


@pytest.fixture(autouse=True)
def _instant_metadata_crawl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip the Akamai-throttle waits for offline tests.

    The production crawl is serial across ~40 databases with a 0.5s
    inter-request delay and 1/2/4s retry backoffs — minutes of real
    ``time.sleep`` under respx. Zeroing the delays keeps every retry and
    backoff branch on the code path while making the suite instant.
    """
    import parsimony_boj.connectors.enumerate as _enum_mod

    monkeypatch.setattr(
        _enum_mod,
        "METADATA_CRAWL",
        MetadataCrawlConfig(
            inter_request_delay_s=0.0,
            retry_statuses=_enum_mod.METADATA_CRAWL.retry_statuses,
            retry_backoffs_s=(0.0, 0.0, 0.0),
        ),
    )

_ENUMERATE_COLS = [
    "code",
    "title",
    "description",
    "db",
    "db_title",
    "entity_type",
    "frequency",
    "unit",
    "category",
    "breadcrumb",
    "start_date",
    "end_date",
    "last_update",
    "source",
]


def test_connectors_collection_exposes_expected_names() -> None:
    """Search connectors register alongside fetch + enumerate without eager catalog I/O."""
    names = {c.name for c in CONNECTORS}
    assert names == {"boj_fetch", "enumerate_boj", "boj_databases_search", "boj_series_search"}


def test_enumerate_output_spec_includes_description_metadata() -> None:
    """``description`` is ordinary metadata in the clean catalog contract."""
    by_name = {c.name: c for c in BOJ_ENUMERATE_OUTPUT.columns}
    assert by_name["description"].role == ColumnRole.METADATA
    assert by_name["source"].role == ColumnRole.METADATA
    assert by_name["entity_type"].role == ColumnRole.METADATA
    assert by_name["db"].role == ColumnRole.METADATA
    assert by_name["frequency"].role == ColumnRole.METADATA


def test_enumerate_columns_match_declared_schema() -> None:
    assert list(_ENUMERATE_COLS) == [c.name for c in BOJ_ENUMERATE_OUTPUT.columns]


def test_resolve_unknown_database_raises_invalid_parameter() -> None:
    """Unknown db codes raise the typed ``InvalidParameterError`` (was a bare
    ``ValueError`` — a programmer-error leak at the connector boundary)."""
    from parsimony_boj import _resolve_boj_database

    with pytest.raises(InvalidParameterError, match="Unknown BoJ database"):
        _resolve_boj_database("ZZ99")


def test_resolve_known_database_normalizes_case() -> None:
    from parsimony_boj import _resolve_boj_database

    code, category, title = _resolve_boj_database("fm08")
    assert code == "FM08"
    assert category == "Financial Markets"
    assert title == "Foreign Exchange Rates"


# ---------------------------------------------------------------------------
# boj_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_boj_fetch_returns_observations() -> None:
    respx.get(_DATA_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "RESULTSET": [
                    {
                        "SERIES_CODE": "FXERD01",
                        "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                        "FREQUENCY": "DAILY",
                        # Survey dates arrive as JSON integers from BoJ.
                        "VALUES": {
                            "SURVEY_DATES": [20260417, 20260418],
                            "VALUES": ["152.33", "152.50"],
                        },
                    }
                ]
            },
        )
    )

    result = boj_fetch(db="FM08", code="FXERD01")

    assert result.provenance.source == "boj_fetch"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "JPY/USD Spot Rate"
    # Integer survey dates parse to ISO and coerce to datetime in the connector body.
    assert df["date"].dtype.kind == "M"
    assert df["value"].tolist() == [152.33, 152.50]


@respx.mock
def test_boj_fetch_skips_null_values_keeps_real_ones() -> None:
    """BoJ pads early periods with ``null`` values; those rows are dropped."""
    respx.get(_DATA_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "RESULTSET": [
                    {
                        "SERIES_CODE": "FXERD01",
                        "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                        "FREQUENCY": "DAILY",
                        "VALUES": {
                            "SURVEY_DATES": [19990101, 19990102, 20260602],
                            "VALUES": [None, None, "159.65"],
                        },
                    }
                ]
            },
        )
    )

    result = boj_fetch(db="FM08", code="FXERD01")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["value"] == 159.65


@respx.mock
def test_boj_fetch_raises_empty_data_on_empty_resultset() -> None:
    respx.get(_DATA_URL).mock(return_value=httpx.Response(200, json={"RESULTSET": []}))

    with pytest.raises(EmptyDataError) as exc:
        boj_fetch(db="FM08", code="XX")
    # EmptyDataError carries the call params for the agent to adjust.
    assert exc.value.query_params == {"db": "FM08", "code": "XX"}


@respx.mock
def test_boj_fetch_raises_empty_data_when_all_values_null() -> None:
    respx.get(_DATA_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "RESULTSET": [
                    {
                        "SERIES_CODE": "FXERD01",
                        "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                        "FREQUENCY": "DAILY",
                        "VALUES": {"SURVEY_DATES": [19990101], "VALUES": [None]},
                    }
                ]
            },
        )
    )

    with pytest.raises(EmptyDataError):
        boj_fetch(db="FM08", code="FXERD01")


@respx.mock
def test_boj_fetch_raises_parse_error_on_non_dict_body() -> None:
    respx.get(_DATA_URL).mock(return_value=httpx.Response(200, json=[1, 2, 3]))

    with pytest.raises(ParseError):
        boj_fetch(db="FM08", code="FXERD01")


def test_boj_fetch_rejects_empty_db() -> None:
    with pytest.raises(InvalidParameterError, match="db must be non-empty"):
        boj_fetch(db="  ", code="FXERD01")


def test_boj_fetch_rejects_empty_code() -> None:
    with pytest.raises(InvalidParameterError, match="At least one series code"):
        boj_fetch(db="FM08", code=" , ")


def test_boj_fetch_rejects_too_many_codes() -> None:
    too_many = ",".join(f"C{i}" for i in range(251))
    with pytest.raises(InvalidParameterError, match="Maximum 250 codes"):
        boj_fetch(db="FM08", code=too_many)


def _series_row(code: str, value: str) -> dict:
    return {
        "SERIES_CODE": code,
        "NAME_OF_TIME_SERIES": f"Series {code}",
        "FREQUENCY": "MONTHLY",
        "VALUES": {"SURVEY_DATES": [202601], "VALUES": [value]},
    }


@respx.mock
def test_boj_fetch_paginates_on_nextposition() -> None:
    """A truncated response (HTTP 200 + ``NEXTPOSITION``) is resumed via
    ``startPosition`` and all series are assembled — no silent data loss."""
    page1 = {
        "RESULTSET": [_series_row("S1", "1.0"), _series_row("S2", "2.0")],
        "NEXTPOSITION": 3,
        "STATUS": 200,
        "MESSAGE": "Successfully completed",
    }
    page2 = {"RESULTSET": [_series_row("S3", "3.0")]}  # no NEXTPOSITION -> done
    respx.get(_DATA_URL).mock(
        side_effect=[httpx.Response(200, json=page1), httpx.Response(200, json=page2)]
    )

    result = boj_fetch(db="FM08", code="S1,S2,S3")
    df = result.data

    assert set(df["code"]) == {"S1", "S2", "S3"}, "pagination dropped series"
    assert len(df) == 3


@respx.mock
def test_boj_fetch_pagination_stops_when_cursor_does_not_advance() -> None:
    """A ``NEXTPOSITION`` that fails to advance halts the loop (no infinite
    pagination). Exactly two responses are provided; a third request would make
    respx raise, so success proves the loop stopped."""
    page1 = {"RESULTSET": [_series_row("S1", "1.0")], "NEXTPOSITION": 2}
    page2 = {"RESULTSET": [_series_row("S2", "2.0")], "NEXTPOSITION": 2}  # does NOT advance
    respx.get(_DATA_URL).mock(
        side_effect=[httpx.Response(200, json=page1), httpx.Response(200, json=page2)]
    )

    result = boj_fetch(db="FM08", code="S1,S2")
    df = result.data
    assert set(df["code"]) == {"S1", "S2"}


def test_database_registry_is_complete_and_canonical() -> None:
    """Floor + shape guards for the frozen archetype-C registry."""
    from parsimony_boj.databases import _BOJ_DATABASES

    codes = [c for c, _cat, _title in _BOJ_DATABASES]
    assert len(_BOJ_DATABASES) == 50, "registry must hold exactly the 50 canonical DBs"
    assert len(set(codes)) == 50, "duplicate DB code in the registry"
    # The historical phantom BP02 must never reappear (the list once drifted).
    assert "BP02" not in codes
    # Spot-check the families the list once missed (FF/CO/BIS/DER/OT were added).
    assert {"FF", "CO", "BIS", "DER", "OT", "FM01", "IR01"}.issubset(codes)
    # Every entry is a well-formed (code, category, title) triple of non-empty strings.
    for code, category, title in _BOJ_DATABASES:
        assert code and category and title, f"malformed registry row for {code!r}"


# ---------------------------------------------------------------------------
# enumerate_boj
# ---------------------------------------------------------------------------


def _stub_metadata_endpoint(*, status: int = 200, json: dict | None = None) -> respx.Route:
    """Mock /getMetadata with a single response for every ``db=`` value."""
    return respx.get(_META_URL).mock(return_value=httpx.Response(status, json=json or {"RESULTSET": []}))


# Realistic FM08 metadata: a section header (empty code, integer LAYER1=1) and
# a series row that belongs under it (LAYER1=1, LAYER2=1).
_FM08_PAYLOAD = {
    "RESULTSET": [
        {
            "SERIES_CODE": "",
            "NAME_OF_TIME_SERIES": "Foreign Exchange Rates (Daily)",
            "LAYER1": 1,
            "LAYER2": 0,
            "LAYER3": 0,
        },
        {
            "SERIES_CODE": "FXERD01",
            "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
            "FREQUENCY": "DAILY",
            "UNIT": "Yen per US dollar",
            "CATEGORY": "Foreign Exchange",
            "START_OF_THE_TIME_SERIES": "19990101",
            "END_OF_THE_TIME_SERIES": "20260602",
            "LAST_UPDATE": "20260604",
            "NOTES": "Tokyo market closing rates.",
            "LAYER1": 1,
            "LAYER2": 1,
            "LAYER3": 0,
        },
    ]
}


@respx.mock
def test_enumerate_boj_emits_series_rows_with_real_breadcrumb() -> None:
    """Series rows carry all 14 columns; breadcrumb resolves from the section
    header's NAME, NOT the integer LAYER ordinal."""
    _stub_metadata_endpoint(json=_FM08_PAYLOAD)

    result = enumerate_boj()
    df = result.data

    assert list(df.columns) == _ENUMERATE_COLS

    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) >= 1

    fx_row = df[df["code"] == "FXERD01"].iloc[0]
    assert fx_row["title"] == "JPY/USD Spot Rate"
    assert fx_row["frequency"] == "Daily"  # _FREQ_MAP normalization
    assert fx_row["unit"] == "Yen per US dollar"
    assert fx_row["source"] == "stat_search"
    assert fx_row["entity_type"] == "series"
    assert fx_row["description"]
    assert "Bank of Japan" in fx_row["description"]
    # Breadcrumb is the section header's NAME — real prose, not "1" / "1 > 1".
    assert fx_row["breadcrumb"] == "Foreign Exchange Rates (Daily)"
    assert "Foreign Exchange Rates (Daily)" in fx_row["description"]

    # Every row carries the constant source token.
    assert set(df["source"]) == {"stat_search"}


@respx.mock
def test_enumerate_boj_breadcrumb_never_a_bare_integer() -> None:
    """Regression: integer LAYER ordinals must not leak into the breadcrumb."""
    _stub_metadata_endpoint(json=_FM08_PAYLOAD)

    result = enumerate_boj()
    series = result.data[result.data["entity_type"] == "series"]
    for bc in series["breadcrumb"]:
        assert not str(bc).strip().isdigit(), f"breadcrumb leaked a raw ordinal: {bc!r}"


@respx.mock
def test_enumerate_boj_emits_db_rows_with_db_prefix_key() -> None:
    """Each DB gets one synthetic ``db:<code>`` row. 50 canonical DBs ⇒ 50 db rows."""
    _stub_metadata_endpoint(json={"RESULTSET": []})

    result = enumerate_boj()
    df = result.data

    db_rows = df[df["entity_type"] == "db"]
    assert len(db_rows) == 50
    assert all(code.startswith("db:") for code in db_rows["code"])
    db_codes_after_prefix = {code[len("db:") :] for code in db_rows["code"]}
    assert {"FF", "CO", "BIS", "DER", "OT", "FM01", "IR01"}.issubset(db_codes_after_prefix)
    # The phantom BP02 must NOT appear in the canonical list.
    assert "BP02" not in db_codes_after_prefix


@respx.mock
def test_enumerate_boj_handles_403_with_retry_then_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Akamai 403s must not crash enumeration. After exhausting retries the
    connector logs a WARNING and proceeds to the next DB."""
    import logging

    respx.get(_META_URL).mock(return_value=httpx.Response(403))

    with caplog.at_level(logging.WARNING, logger="parsimony_boj"):
        result = enumerate_boj()

    df = result.data
    assert list(df.columns) == _ENUMERATE_COLS
    warning_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("failed metadata fetch" in m.lower() for m in warning_messages)


@respx.mock
def test_enumerate_boj_entities_from_raw_round_trip() -> None:
    """The enumerator Result projects to catalog entities with KEY=code (ns boj)."""
    _stub_metadata_endpoint(json=_FM08_PAYLOAD)

    result = enumerate_boj()
    # The respx stub returns the same payload for every DB; dedupe on code.
    frame = result.data.drop_duplicates(subset=["code"], keep="first")
    entries = entities_from_raw(frame, BOJ_ENUMERATE_OUTPUT)

    by_code = {e.code: e for e in entries}
    series_entry = by_code["FXERD01"]
    assert series_entry.namespace == "boj"
    assert series_entry.title == "JPY/USD Spot Rate"
    assert series_entry.metadata.get("description")
    assert series_entry.metadata.get("source") == "stat_search"
    assert series_entry.metadata.get("entity_type") == "series"
    assert series_entry.metadata.get("frequency") == "Daily"
    assert series_entry.metadata.get("unit") == "Yen per US dollar"

    db_entries = [e for e in entries if e.code.startswith("db:")]
    assert len(db_entries) >= 1
    db_entry = db_entries[0]
    assert db_entry.namespace == "boj"
    assert db_entry.metadata.get("entity_type") == "db"
    assert db_entry.metadata.get("source") == "stat_search"
