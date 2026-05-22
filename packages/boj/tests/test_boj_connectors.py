"""Happy-path tests for the Bank of Japan connectors.

BoJ is public (no api_key); template 401/429 contract does not apply. BoJ
constructs an httpx.AsyncClient directly (not the kernel HttpClient) so
respx still hooks into the transport.
"""

from __future__ import annotations

import logging

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError
from parsimony_test_support import entries_result_to_dataframe

from parsimony_boj import (
    BOJ_ENUMERATE_OUTPUT,
    CONNECTORS,
    boj_fetch,
    enumerate_boj,
)


def test_connectors_collection_exposes_expected_names() -> None:
    """``boj_search`` is a registered connector alongside fetch + enumerate.

    We include it in the expected set because it ships with the package
    and is what agents call to navigate the catalog. ``Catalog.load``
    is lazy (only invoked on first ``boj_search`` call), so import-time
    registration succeeds without any network or HF access.
    """
    names = {c.name for c in CONNECTORS}
    assert names == {"boj_fetch", "enumerate_boj", "boj_search"}


def test_enumerate_output_schema_includes_description_metadata() -> None:
    """``description`` is ordinary metadata in the clean catalog contract."""
    from parsimony.result import ColumnRole

    by_name = {c.name: c for c in BOJ_ENUMERATE_OUTPUT.columns}
    assert by_name["description"].role == ColumnRole.METADATA
    assert by_name["source"].role == ColumnRole.METADATA
    assert by_name["entity_type"].role == ColumnRole.METADATA
    assert by_name["db"].role == ColumnRole.METADATA
    assert by_name["frequency"].role == ColumnRole.METADATA


@respx.mock
@pytest.mark.asyncio
async def test_boj_fetch_returns_observations() -> None:
    respx.get("https://www.stat-search.boj.or.jp/api/v1/getDataCode").mock(
        return_value=httpx.Response(
            200,
            json={
                "RESULTSET": [
                    {
                        "SERIES_CODE": "FXERD01",
                        "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                        "FREQUENCY": "DM",
                        "VALUES": {
                            "SURVEY_DATES": ["20260417", "20260418"],
                            "VALUES": ["152.33", "152.50"],
                        },
                    }
                ]
            },
        )
    )

    result = await boj_fetch(db="FM08", code="FXERD01")

    assert result.provenance.source == "boj_fetch"
    df = entries_result_to_dataframe(result)
    assert len(df) == 2
    assert df.iloc[0]["title"] == "JPY/USD Spot Rate"


@respx.mock
@pytest.mark.asyncio
async def test_boj_fetch_raises_empty_data_on_empty_resultset() -> None:
    respx.get("https://www.stat-search.boj.or.jp/api/v1/getDataCode").mock(
        return_value=httpx.Response(200, json={"RESULTSET": []})
    )

    with pytest.raises(EmptyDataError):
        await boj_fetch(db="FM08", code="XX")


# ---------------------------------------------------------------------------
# enumerate_boj
# ---------------------------------------------------------------------------


def _stub_metadata_endpoint(*, status: int = 200, json: dict | None = None) -> respx.Route:
    """Mock the /getMetadata endpoint with a single response.

    Catches every ``db=`` value so the per-DB fan-out lands here. Tests
    that need per-DB behaviour can override after this.
    """
    return respx.get("https://www.stat-search.boj.or.jp/api/v1/getMetadata").mock(
        return_value=httpx.Response(status, json=json or {"RESULTSET": []})
    )


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boj_emits_one_row_per_series_with_description_and_source() -> None:
    """Every series row must have all 13 columns populated and source metadata."""
    payload = {
        "RESULTSET": [
            # Layer header for breadcrumb context.
            {"SERIES_CODE": "", "LAYER1": "Foreign Exchange Rates", "LAYER2": "Spot"},
            {
                "SERIES_CODE": "FXERD01",
                "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                "FREQUENCY": "DAILY",
                "UNIT": "Yen per US dollar",
                "CATEGORY": "Foreign Exchange",
                "START_OF_THE_TIME_SERIES": "1980-01-01",
                "END_OF_THE_TIME_SERIES": "2026-04-24",
                "LAST_UPDATE": "2026-04-25",
                "NOTES": "Tokyo market closing rates.",
            },
        ]
    }
    _stub_metadata_endpoint(json=payload)

    result = await enumerate_boj()
    df = entries_result_to_dataframe(result)

    series_rows = df[df["entity_type"] == "series"]
    assert len(series_rows) >= 1

    # All 13 enumerate columns are present and in the expected order.
    expected_cols = [
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
    assert list(df.columns) == expected_cols

    fx_row = df[df["code"] == "FXERD01"].iloc[0]
    assert fx_row["title"] == "JPY/USD Spot Rate"
    assert fx_row["frequency"] == "Daily"  # _FREQ_MAP normalization
    assert fx_row["unit"] == "Yen per US dollar"
    assert fx_row["source"] == "stat_search"
    assert fx_row["entity_type"] == "series"
    assert fx_row["description"]  # non-empty
    assert "Bank of Japan" in fx_row["description"]
    # Breadcrumb propagates from preceding layer header rows.
    assert "Foreign Exchange Rates" in fx_row["breadcrumb"]

    # Every row carries the constant source token.
    assert set(df["source"]) == {"stat_search"}


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boj_emits_db_rows_with_db_prefix_key() -> None:
    """Each DB gets one synthetic row keyed ``db:<code>`` so agents can
    discover whole databases via semantic search (mirrors BoC's
    ``group:`` pattern). 50 canonical DBs (per the official API manual)
    ⇒ exactly 50 db rows.
    """
    _stub_metadata_endpoint(json={"RESULTSET": []})

    result = await enumerate_boj()
    df = entries_result_to_dataframe(result)

    db_rows = df[df["entity_type"] == "db"]
    assert len(db_rows) == 50
    assert all(code.startswith("db:") for code in db_rows["code"])
    # Spot-check a few canonical DBs that were either added (FF, CO, BIS,
    # DER, OT) or already present (FM01, IR01).
    db_codes_after_prefix = {code[len("db:"):] for code in db_rows["code"]}
    assert {"FF", "CO", "BIS", "DER", "OT", "FM01", "IR01"}.issubset(db_codes_after_prefix)
    # The phantom BP02 must NOT appear in the canonical list.
    assert "BP02" not in db_codes_after_prefix


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boj_handles_403_with_retry_then_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Akamai 403s must not crash enumeration. The connector retries with
    exponential backoff and, after exhausting retries, logs a WARNING and
    proceeds to the next DB.
    """
    respx.get("https://www.stat-search.boj.or.jp/api/v1/getMetadata").mock(
        return_value=httpx.Response(403)
    )

    with caplog.at_level(logging.WARNING, logger="parsimony_boj"):
        result = await enumerate_boj()

    # The DataFrame remains rectangular even when every DB fails.
    expected_cols = [
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
    df = entries_result_to_dataframe(result, columns=expected_cols)
    # No series rows came through (every DB 403'd) but DB rows are still
    # absent because the connector emits them only for DBs whose metadata
    # was successfully retrieved. The summary log line is what we assert.
    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("metadata fetch failed" in m.lower() for m in warning_messages)
    assert list(df.columns) == expected_cols


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boj_emits_columns_required_for_catalog_entries() -> None:
    """The Result returned by the enumerator must carry catalog entries
    with exactly one KEY (code), one TITLE (title), and METADATA columns
    for the BoJ-specific dispatch hints.
    """
    payload = {
        "RESULTSET": [
            {"SERIES_CODE": "", "LAYER1": "Spot Rates"},
            {
                "SERIES_CODE": "FXERD01",
                "NAME_OF_TIME_SERIES": "JPY/USD Spot Rate",
                "FREQUENCY": "DAILY",
                "UNIT": "Yen per USD",
                "CATEGORY": "Foreign Exchange",
                "START_OF_THE_TIME_SERIES": "1980-01-01",
                "END_OF_THE_TIME_SERIES": "2026-04-24",
                "LAST_UPDATE": "2026-04-25",
            },
        ]
    }
    _stub_metadata_endpoint(json=payload)

    result = await enumerate_boj()
    entries = result.data

    by_code = {e.code: e for e in entries}
    series_entry = by_code["FXERD01"]
    assert series_entry.namespace == "boj"
    assert series_entry.title == "JPY/USD Spot Rate"
    assert series_entry.metadata.get("description")
    assert series_entry.metadata.get("source") == "stat_search"
    assert series_entry.metadata.get("entity_type") == "series"
    assert series_entry.metadata.get("frequency") == "Daily"
    assert series_entry.metadata.get("unit") == "Yen per USD"

    # At least one DB-level row is also present and well-formed.
    db_entries = [e for e in entries if e.code.startswith("db:")]
    assert len(db_entries) >= 1
    db_entry = db_entries[0]
    assert db_entry.namespace == "boj"
    assert db_entry.metadata.get("entity_type") == "db"
    assert db_entry.metadata.get("source") == "stat_search"
