"""Happy-path tests for the Destatis connectors.

The legacy ``/genesisWS/rest/2020/*`` API is retired upstream and now
redirects to an HTML announcement page. Tests target the new public
``/genesisGONLINE/api/rest/*`` endpoints:

* ``destatis_fetch`` — JSON-stat 2.0 over ``/tables/{code}/data``
* ``enumerate_destatis`` — composes ``/statistics``,
  ``/statistics/{code}/information``, and ``/statistics/{code}/tables``
* ``destatis_search`` — semantic search over the published catalog (lazy
  ``Catalog.from_url`` keeps import-time cheap, so it registers without
  any network)

The new API is anonymous; ``DESTATIS_USERNAME`` / ``DESTATIS_PASSWORD``
remain as no-op env vars for backward compatibility.
"""

from __future__ import annotations

import logging

import httpx
import pytest
import respx
from parsimony.errors import ProviderError
from parsimony.result import ColumnRole

from parsimony_destatis import (
    CONNECTORS,
    DESTATIS_ENUMERATE_OUTPUT,
    DestatisEnumerateParams,
    DestatisFetchParams,
    destatis_fetch,
    enumerate_destatis,
)

_BASE = "https://www-genesis.destatis.de/genesisGONLINE/api/rest"


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


def test_env_vars_maps_username_and_password() -> None:
    """``username``/``password`` are no-op on the new API but the env-var
    contract is preserved so existing deployments don't have to drop the
    secrets from their config.
    """
    assert CONNECTORS["destatis_fetch"].env_map == {
        "username": "DESTATIS_USERNAME",
        "password": "DESTATIS_PASSWORD",
    }


def test_connectors_collection_exposes_expected_names() -> None:
    """Three connectors ship with the package: fetch, enumerate, and the
    semantic-search tool that maps natural-language queries to codes.
    ``Catalog.from_url`` is lazy (only invoked on first ``destatis_search``
    call), so import-time registration succeeds without any network or HF
    access.
    """
    names = {c.name for c in CONNECTORS}
    assert names == {"destatis_fetch", "enumerate_destatis", "destatis_search"}


def test_enumerate_output_schema_includes_description_role() -> None:
    """``description`` must be routed via DESCRIPTION (semantic_text) not
    METADATA (BM25 only) so the multilingual embedder picks it up.
    Mirrors BoJ / BoC.
    """
    by_name = {c.name: c for c in DESTATIS_ENUMERATE_OUTPUT.columns}
    assert by_name["description"].role == ColumnRole.DESCRIPTION
    assert by_name["source"].role == ColumnRole.METADATA
    assert by_name["entity_type"].role == ColumnRole.METADATA
    assert by_name["parent_statistic"].role == ColumnRole.METADATA
    assert by_name["title_de"].role == ColumnRole.METADATA
    assert by_name["title_en"].role == ColumnRole.METADATA


# ---------------------------------------------------------------------------
# destatis_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_parses_jsonstat_response() -> None:
    """Happy-path: 2-cell JSON-stat dataset → 2-row long DataFrame with
    parsed German-month dates and float values.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(200, json=_JSONSTAT_FIXTURE)
    )

    result = await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))

    assert result.provenance.source == "destatis"
    df = result.data
    assert len(df) == 2
    assert "series_id" in df.columns
    assert df.iloc[0]["series_id"] == "61111-0001"
    # The new GENESIS-Online API doesn't expose a top-level dataset label,
    # so the parser falls back to the table code.
    assert df.iloc[0]["title"] == "61111-0001"
    # Dates parsed via ``_normalize_german_date`` (German month names).
    assert set(df["date"]) == {"2026-01-01", "2026-02-01"}
    assert sorted(df["value"].tolist()) == [108.4, 108.7]


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_accepts_legacy_table_id_alias() -> None:
    """``table_id=`` keeps working via the pydantic alias so callers that
    upgrade in two steps (rename later) don't break.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(200, json=_JSONSTAT_FIXTURE)
    )

    # Legacy keyword ``table_id=`` resolves via populate_by_name.
    result = await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))
    assert result.provenance.source == "destatis"


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_maps_500_to_provider_error() -> None:
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(500, text="upstream error")
    )

    with pytest.raises(ProviderError):
        await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))


@respx.mock
@pytest.mark.asyncio
async def test_destatis_fetch_raises_provider_error_on_announcement_redirect() -> None:
    """Safety net: if Destatis ever swaps us back onto the SPA shell, we
    surface a clear "API may have changed" error rather than letting the
    JSON parser blow up on HTML.
    """
    respx.get(f"{_BASE}/tables/61111-0001/data").mock(
        return_value=httpx.Response(
            200, text="<html><body>Wartungsarbeiten announcement</body></html>"
        )
    )

    with pytest.raises(ProviderError, match="API may have changed"):
        await destatis_fetch(DestatisFetchParams(table_id="61111-0001"))


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


def test_fetch_requires_table_id() -> None:
    """``name=`` (the canonical field) must be non-empty."""
    with pytest.raises(ValueError):
        DestatisFetchParams(table_id="")


# ---------------------------------------------------------------------------
# enumerate_destatis
# ---------------------------------------------------------------------------


def _stub_index(statistics: list[dict]) -> respx.Route:
    return respx.get(f"{_BASE}/statistics").mock(
        return_value=httpx.Response(200, json={"statistics": statistics})
    )


def _stub_information(code: str, *, status: int = 200, payload: dict | None = None) -> respx.Route:
    return respx.get(f"{_BASE}/statistics/{code}/information").mock(
        return_value=httpx.Response(status, json=payload or {})
    )


def _stub_tables(code: str, *, status: int = 200, tables: list[dict] | None = None) -> respx.Route:
    return respx.get(f"{_BASE}/statistics/{code}/tables").mock(
        return_value=httpx.Response(status, json={"tables": tables or []})
    )


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_destatis_emits_statistic_and_table_rows() -> None:
    """Round-trip through the three-call composition: ``/statistics`` ⇒
    one statistic row + one row per table from
    ``/statistics/{code}/tables``. Both rows must carry a non-empty
    DESCRIPTION (the embedder input).
    """
    _stub_index([
        {
            "code": "61111",
            "name": {"de": "Verbraucherpreisindex", "en": "Consumer price index"},
            "subjectArea": "Preise",
        }
    ])
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
                "variables": [
                    {"code": "PREIS1", "name": {"de": "Index", "en": "Index"}},
                    {"code": "ZEIT", "name": {"de": "Zeit", "en": "Time"}},
                ],
            }
        ],
    )

    result = await enumerate_destatis(DestatisEnumerateParams())
    df = result.data

    # Exactly the 11-column schema, in declared order.
    expected_cols = [
        "code",
        "title",
        "description",
        "entity_type",
        "parent_statistic",
        "subject_area",
        "title_de",
        "title_en",
        "variable_codes",
        "variable_names_en",
        "source",
    ]
    assert list(df.columns) == expected_cols

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

    table = table_rows.iloc[0]
    assert table["code"] == "61111-0001"
    assert table["title"] == "CPI: Germany, monthly"
    assert table["parent_statistic"] == "61111"
    assert table["variable_codes"] == "PREIS1,ZEIT"
    assert table["variable_names_en"] == "Index,Time"
    assert table["source"] == "genesis_online"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_destatis_lifts_parent_description_into_table_rows() -> None:
    """Per-table semantic queries need the long parent description as
    retrieval signal — table titles alone (e.g. "Index nach Bundesländern,
    Monate") are too thin for the embedder to disambiguate.
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

    result = await enumerate_destatis(DestatisEnumerateParams())
    df = result.data

    table_rows = df[df["entity_type"] == "table"]
    assert len(table_rows) == 2
    for _, row in table_rows.iterrows():
        # The parent's German lead paragraph is lifted into every table row.
        assert "Preisentwicklung" in row["description"]
        assert "Parent statistic: CPI (61111)" in row["description"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_destatis_handles_429_with_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per-statistic 429s must not crash enumeration. After exhausting
    retries we WARN and proceed; the index call is mocked separately so a
    failed statistic doesn't hide the rest of the catalog.
    """
    _stub_index([{"code": "61111", "name": {"de": "VPI", "en": "CPI"}}])
    # Both per-statistic calls 429 every time — exercises the retry path
    # to exhaustion.
    respx.get(f"{_BASE}/statistics/61111/information").mock(
        return_value=httpx.Response(429)
    )
    respx.get(f"{_BASE}/statistics/61111/tables").mock(
        return_value=httpx.Response(429)
    )

    with caplog.at_level(logging.WARNING, logger="parsimony_destatis"):
        result = await enumerate_destatis(DestatisEnumerateParams())

    df = result.data
    # No rows came through (the only statistic 429'd both endpoints) but
    # the schema is still rectangular and the warning is logged.
    expected_cols = [
        "code",
        "title",
        "description",
        "entity_type",
        "parent_statistic",
        "subject_area",
        "title_de",
        "title_en",
        "variable_codes",
        "variable_names_en",
        "source",
    ]
    assert list(df.columns) == expected_cols

    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("fetch failed" in m.lower() for m in warning_messages)


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_destatis_emits_columns_required_for_catalog_entries() -> None:
    """The ``Result`` returned by the enumerator must carry an output
    schema that ``entries_from_result`` accepts: exactly one KEY (code),
    one TITLE (title), one DESCRIPTION (description), and METADATA columns
    for the Destatis-specific dispatch hints.
    """
    from parsimony.catalog import entries_from_result

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
        tables=[
            {"code": "61111-0001", "name": {"de": "VPI Monate", "en": "CPI monthly"}}
        ],
    )

    result = await enumerate_destatis(DestatisEnumerateParams())
    entries = entries_from_result(result)

    by_code = {e.code: e for e in entries}
    assert "61111" in by_code
    assert "61111-0001" in by_code

    stat_entry = by_code["61111"]
    assert stat_entry.namespace == "destatis"
    assert stat_entry.title == "CPI"
    assert stat_entry.description  # non-empty — feeds semantic_text()
    assert stat_entry.metadata.get("entity_type") == "statistic"
    assert stat_entry.metadata.get("source") == "genesis_online"

    table_entry = by_code["61111-0001"]
    assert table_entry.namespace == "destatis"
    assert table_entry.metadata.get("entity_type") == "table"
    assert table_entry.metadata.get("parent_statistic") == "61111"
    assert table_entry.metadata.get("source") == "genesis_online"
