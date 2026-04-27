"""Happy-path tests for the Bank of Canada Valet connectors.

Public API, no api_key; template 401/429 contract does not apply. BoC
constructs an httpx.AsyncClient directly (not the kernel HttpClient) so
respx still hooks into the transport.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_boc import (
    CONNECTORS,
    BocEnumerateParams,
    BocFetchParams,
    boc_fetch,
    enumerate_boc,
)


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"boc_fetch", "enumerate_boc"}


def test_enumerate_output_schema_routes_description_via_description_role() -> None:
    """Upstream ``description`` text must reach DESCRIPTION (semantic_text),
    not METADATA (BM25 only). Mirrors Treasury's ``definition`` column.
    """
    from parsimony.result import ColumnRole
    from parsimony_boc import BOC_ENUMERATE_OUTPUT

    by_name = {c.name: c for c in BOC_ENUMERATE_OUTPUT.columns}
    assert by_name["description"].role == ColumnRole.DESCRIPTION
    assert by_name["source"].role == ColumnRole.METADATA
    assert by_name["entity_type"].role == ColumnRole.METADATA
    assert by_name["group"].role == ColumnRole.METADATA


@respx.mock
@pytest.mark.asyncio
async def test_boc_fetch_single_series_returns_observations() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriesDetail": {
                    "FXUSDCAD": {"label": "USD/CAD", "description": "US dollar to Canadian dollar"}
                },
                "observations": [
                    {"d": "2026-04-17", "FXUSDCAD": {"v": "1.3852"}},
                    {"d": "2026-04-18", "FXUSDCAD": {"v": "1.3840"}},
                ],
            },
        )
    )

    result = await boc_fetch(BocFetchParams(series_name="FXUSDCAD"))

    assert result.provenance.source == "boc"
    df = result.data
    assert len(df) >= 1


@respx.mock
@pytest.mark.asyncio
async def test_boc_fetch_group_syntax_uses_group_endpoint() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriesDetail": {
                    "FXUSDCAD": {"label": "USD/CAD"},
                    "FXEURCAD": {"label": "EUR/CAD"},
                },
                "observations": [
                    {
                        "d": "2026-04-18",
                        "FXUSDCAD": {"v": "1.3840"},
                        "FXEURCAD": {"v": "1.4720"},
                    },
                ],
            },
        )
    )

    result = await boc_fetch(BocFetchParams(series_name="group:FX_RATES_DAILY"))

    assert result.provenance.source == "boc"
    assert len(result.data) >= 1


@respx.mock
@pytest.mark.asyncio
async def test_boc_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/XX/json").mock(
        return_value=httpx.Response(
            200, json={"seriesDetail": {"XX": {"label": "x"}}, "observations": []}
        )
    )

    with pytest.raises(EmptyDataError):
        await boc_fetch(BocFetchParams(series_name="XX"))


def test_fetch_rejects_empty_series_name() -> None:
    with pytest.raises(ValueError):
        BocFetchParams(series_name="   ")


# ---------------------------------------------------------------------------
# enumerate_boc
# ---------------------------------------------------------------------------


def _mock_enumerate_endpoints(
    *,
    series_payload: dict,
    groups_payload: dict,
    group_membership: dict[str, dict],
) -> None:
    """Wire respx routes for the three Valet list endpoints.

    ``group_membership`` maps group_name → ``groupDetails`` body. Any group
    listed in ``groups_payload`` that is missing from this map is wired to
    return an empty membership response, so tests can be selective.
    """
    respx.get("https://www.bankofcanada.ca/valet/lists/series/json").mock(
        return_value=httpx.Response(200, json=series_payload)
    )
    respx.get("https://www.bankofcanada.ca/valet/lists/groups/json").mock(
        return_value=httpx.Response(200, json=groups_payload)
    )
    for group_name in (groups_payload.get("groups") or {}):
        details = group_membership.get(
            group_name,
            {"name": group_name, "groupSeries": {}},
        )
        respx.get(
            f"https://www.bankofcanada.ca/valet/groups/{group_name}/json"
        ).mock(return_value=httpx.Response(200, json={"groupDetails": details}))


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_emits_one_row_per_series_with_description_and_source() -> None:
    """The upstream ``description`` field must populate the DESCRIPTION
    column (not the legacy ``group`` METADATA column), and every row must
    carry ``source='valet'`` for dispatch."""
    _mock_enumerate_endpoints(
        series_payload={
            "series": {
                "FXUSDCAD": {
                    "label": "USD/CAD",
                    "description": "US dollar to Canadian dollar daily exchange rate",
                },
                "V39079": {
                    "label": "Government of Canada benchmark bond yields - 10 year",
                    "description": "GoC 10-year benchmark bond yield",
                },
            }
        },
        groups_payload={
            "groups": {
                "FX_RATES_DAILY": {
                    "label": "Daily exchange rates",
                },
                "BOND_YIELDS_BENCHMARK": {
                    "label": "Government of Canada benchmark bond yields",
                },
            }
        },
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "label": "Daily exchange rates",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
            "BOND_YIELDS_BENCHMARK": {
                "name": "BOND_YIELDS_BENCHMARK",
                "label": "Government of Canada benchmark bond yields",
                "groupSeries": {"V39079": {"label": "10 Year"}},
            },
        },
    )

    result = await enumerate_boc(BocEnumerateParams())
    df = result.data

    # Per-series rows are emitted alongside group rows (groups are
    # discoverable as their own catalog entries via the ``group:`` prefix).
    series_rows = df[df["entity_type"] == "series"]
    assert set(series_rows["series_name"]) == {"FXUSDCAD", "V39079"}

    fxusdcad = df[df["series_name"] == "FXUSDCAD"].iloc[0]
    assert fxusdcad["title"] == "USD/CAD"
    # ``description`` carries the upstream description text — the bug
    # being fixed was that it was previously stuffed into ``group``.
    assert fxusdcad["description"] == "US dollar to Canadian dollar daily exchange rate"
    assert fxusdcad["source"] == "valet"
    assert fxusdcad["entity_type"] == "series"

    # Every row carries source='valet' — future-proofing for a parallel
    # data source so dispatch is already wired.
    assert set(df["source"]) == {"valet"}


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_group_metadata_is_group_id_not_description() -> None:
    """The ``group`` column carries the upstream group identifier (e.g.
    ``FX_RATES_DAILY``), not the upstream description text. This is the
    regression target — the previous implementation stored the
    description in ``group``."""
    _mock_enumerate_endpoints(
        series_payload={
            "series": {
                "FXUSDCAD": {
                    "label": "USD/CAD",
                    "description": "US dollar to Canadian dollar daily exchange rate",
                },
            }
        },
        groups_payload={
            "groups": {
                "FX_RATES_DAILY": {"label": "Daily exchange rates"},
            }
        },
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
        },
    )

    df = (await enumerate_boc(BocEnumerateParams())).data
    row = df[df["series_name"] == "FXUSDCAD"].iloc[0]

    assert row["group"] == "FX_RATES_DAILY"
    assert row["group_label"] == "Daily exchange rates"
    # The upstream description text must NOT leak into the group column.
    assert "Canadian dollar" not in row["group"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_series_with_no_group_membership_has_empty_group() -> None:
    """Series that aren't members of any catalogued group end up with an
    empty ``group`` field rather than missing/None — keeps the column
    rectangular for downstream consumers."""
    _mock_enumerate_endpoints(
        series_payload={
            "series": {
                "ORPHAN_SERIES": {
                    "label": "Orphan series",
                    "description": "A series in no group at all",
                },
            }
        },
        groups_payload={
            "groups": {
                "FX_RATES_DAILY": {"label": "Daily exchange rates"},
            }
        },
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
        },
    )

    df = (await enumerate_boc(BocEnumerateParams())).data
    row = df[df["series_name"] == "ORPHAN_SERIES"].iloc[0]
    assert row["group"] == ""
    assert row["group_label"] == ""
    # DESCRIPTION column is still populated even when group membership is missing.
    assert row["description"] == "A series in no group at all"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_keeps_first_group_when_series_in_multiple_groups() -> None:
    """Multi-group membership is rare. When it occurs, the first group
    encountered (in the order BoC returns groups in /lists/groups/json)
    wins. Tests the deterministic tie-break."""
    _mock_enumerate_endpoints(
        series_payload={
            "series": {
                "FXUSDCAD": {
                    "label": "USD/CAD",
                    "description": "USD to CAD",
                },
            }
        },
        groups_payload={
            # dict insertion order = iteration order in Python 3.7+
            "groups": {
                "FX_RATES_DAILY": {"label": "Daily exchange rates"},
                "FX_RATES_ALTERNATIVE": {"label": "Alternative exchange rates"},
            }
        },
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
            "FX_RATES_ALTERNATIVE": {
                "name": "FX_RATES_ALTERNATIVE",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD alt"}},
            },
        },
    )

    df = (await enumerate_boc(BocEnumerateParams())).data
    row = df[df["series_name"] == "FXUSDCAD"].iloc[0]
    assert row["group"] == "FX_RATES_DAILY"
    assert row["group_label"] == "Daily exchange rates"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_tolerates_failing_group_endpoint() -> None:
    """A flaky /groups/{name}/json call must not fail the whole sweep —
    the group's series simply lose their membership info, but every other
    row is preserved. BoC has been known to leave dead group IDs in the
    catalogue."""
    respx.get("https://www.bankofcanada.ca/valet/lists/series/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "series": {
                    "FXUSDCAD": {
                        "label": "USD/CAD",
                        "description": "USD to CAD",
                    },
                }
            },
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/lists/groups/json").mock(
        return_value=httpx.Response(
            200,
            json={"groups": {"DEAD_GROUP": {"label": "Retired group"}}},
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/groups/DEAD_GROUP/json").mock(
        return_value=httpx.Response(503)
    )

    df = (await enumerate_boc(BocEnumerateParams())).data
    # Series rows: one (FXUSDCAD). Plus a group row for DEAD_GROUP — the
    # group enumeration is independent of the per-group membership
    # fan-out, so a 503 on /groups/{name}/json strips membership info
    # but the group itself is still catalogued from /lists/groups/json.
    series_rows = df[df["entity_type"] == "series"]
    assert set(series_rows["series_name"]) == {"FXUSDCAD"}
    row = series_rows.iloc[0]
    assert row["group"] == ""
    assert row["description"] == "USD to CAD"
    group_rows = df[df["entity_type"] == "group"]
    assert set(group_rows["series_name"]) == {"group:DEAD_GROUP"}


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_emits_one_row_per_group_with_group_prefix_key() -> None:
    """Groups are addressable entities (via ``boc_fetch(series_name='group:NAME')``)
    and should appear in the catalog as their own rows so agents can
    discover whole panels via group-level descriptions (e.g.
    'Month-end, Millions of dollars' is uniquely a group-level signal).
    """
    _mock_enumerate_endpoints(
        series_payload={
            "series": {
                "FXUSDCAD": {"label": "USD/CAD", "description": "USD to CAD"},
            }
        },
        groups_payload={
            "groups": {
                "FX_RATES_DAILY": {
                    "label": "Daily exchange rates",
                    "description": "Daily average exchange rates - published once each business day by 16:30 ET.",
                },
                "A4_FUNDS_CONSUMER": {
                    "label": "Funds advanced — consumer credit",
                    "description": "Month-end, Millions of dollars",
                },
            }
        },
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
        },
    )

    df = (await enumerate_boc(BocEnumerateParams())).data

    group_rows = df[df["entity_type"] == "group"]
    assert set(group_rows["series_name"]) == {
        "group:FX_RATES_DAILY",
        "group:A4_FUNDS_CONSUMER",
    }
    fx_group = df[df["series_name"] == "group:FX_RATES_DAILY"].iloc[0]
    assert fx_group["title"] == "Daily exchange rates"
    # Group description text is the most useful retrieval signal at the
    # group level — units, frequency, methodology hints. Must reach the
    # DESCRIPTION column so the embedder indexes it.
    assert "Daily average exchange rates" in fx_group["description"]
    assert fx_group["source"] == "valet"
    assert fx_group["group"] == "FX_RATES_DAILY"
    assert fx_group["group_label"] == "Daily exchange rates"

    # The economically-meaningful "Month-end, Millions of dollars" hint is
    # only available at the group level — no series carries it.
    funds_group = df[df["series_name"] == "group:A4_FUNDS_CONSUMER"].iloc[0]
    assert funds_group["description"] == "Month-end, Millions of dollars"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_boc_emits_columns_required_for_catalog_entries() -> None:
    """The Result returned by the enumerator must carry an output schema
    that ``entries_from_result`` accepts: exactly one KEY (series_name),
    one TITLE (title), one DESCRIPTION (description), and METADATA
    columns for source/group/group_label.
    """
    from parsimony.catalog import entries_from_result

    _mock_enumerate_endpoints(
        series_payload={
            "series": {
                "FXUSDCAD": {
                    "label": "USD/CAD",
                    "description": "US dollar to Canadian dollar daily exchange rate",
                },
            }
        },
        groups_payload={
            "groups": {
                "FX_RATES_DAILY": {"label": "Daily exchange rates"},
            }
        },
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
        },
    )

    result = await enumerate_boc(BocEnumerateParams())
    entries = entries_from_result(result)
    # One series row plus one group row. Groups are catalogued as their
    # own discoverable entities so agents can find them via group-level
    # description text.
    assert len(entries) == 2
    by_code = {e.code: e for e in entries}
    series_entry = by_code["FXUSDCAD"]
    assert series_entry.namespace == "boc"
    assert series_entry.title == "USD/CAD"
    # DESCRIPTION column flows into SeriesEntry.description (and thus
    # into semantic_text() at indexing time).
    assert series_entry.description == "US dollar to Canadian dollar daily exchange rate"
    # METADATA columns flow into SeriesEntry.metadata.
    assert series_entry.metadata.get("source") == "valet"
    assert series_entry.metadata.get("entity_type") == "series"
    assert series_entry.metadata.get("group") == "FX_RATES_DAILY"
    assert series_entry.metadata.get("group_label") == "Daily exchange rates"

    group_entry = by_code["group:FX_RATES_DAILY"]
    assert group_entry.namespace == "boc"
    assert group_entry.title == "Daily exchange rates"
    assert group_entry.metadata.get("entity_type") == "group"
    assert group_entry.metadata.get("group") == "FX_RATES_DAILY"
