"""Happy-path tests for the Bank of Canada Valet connectors.

Public API, no api_key; template 401/429 contract does not apply. BoC fetches
go through the kernel ``HttpClient`` (httpx under the hood) so respx still hooks
into the transport.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import Result

from parsimony_boc import (
    BOC_ENUMERATE_OUTPUT,
    CONNECTORS,
    boc_fetch,
    enumerate_boc,
)


def _enumerate_dataframe(result: Result) -> pd.DataFrame:
    assert isinstance(result.data, pd.DataFrame)
    return result.data


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"boc_fetch", "enumerate_boc", "boc_search"}


def test_enumerate_output_spec_routes_description_as_metadata() -> None:
    """Upstream ``description`` text is ordinary catalog metadata."""
    from parsimony.result import ColumnRole

    from parsimony_boc import BOC_ENUMERATE_OUTPUT

    by_name = {c.name: c for c in BOC_ENUMERATE_OUTPUT.columns}
    assert by_name["description"].role == ColumnRole.METADATA
    assert by_name["source"].role == ColumnRole.METADATA
    assert by_name["entity_type"].role == ColumnRole.METADATA
    assert by_name["group"].role == ColumnRole.METADATA


@respx.mock
def test_boc_fetch_single_series_returns_observations() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriesDetail": {"FXUSDCAD": {"label": "USD/CAD", "description": "US dollar to Canadian dollar"}},
                "observations": [
                    {"d": "2026-04-17", "FXUSDCAD": {"v": "1.3852"}},
                    {"d": "2026-04-18", "FXUSDCAD": {"v": "1.3840"}},
                ],
            },
        )
    )

    result = boc_fetch(series_name="FXUSDCAD")

    assert result.provenance.source == "boc_fetch"
    df = result.data
    assert len(df) == 2
    assert set(df["series_name"]) == {"FXUSDCAD"}
    assert df["title"].iloc[0] == "USD/CAD"
    # Values parse to real numerics (coerced in boc_fetch).
    assert df["value"].dtype.kind == "f"
    assert df["value"].tolist() == [1.3852, 1.3840]
    # Dates parse to real datetimes (coerced in boc_fetch).
    assert df["date"].dtype.kind == "M"


@respx.mock
def test_boc_fetch_group_syntax_uses_group_endpoint() -> None:
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

    result = boc_fetch(series_name="group:FX_RATES_DAILY")

    assert result.provenance.source == "boc_fetch"
    df = result.data
    # Both series in the group panel are melted into long format.
    assert set(df["series_name"]) == {"FXUSDCAD", "FXEURCAD"}
    assert df["value"].notna().all()


@respx.mock
def test_boc_fetch_passes_date_window_params() -> None:
    """``start_date``/``end_date`` reach the wire; ``None`` values are dropped."""
    route = respx.get("https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "seriesDetail": {"FXUSDCAD": {"label": "USD/CAD"}},
                "observations": [{"d": "2024-01-02", "FXUSDCAD": {"v": "1.3316"}}],
            },
        )
    )

    boc_fetch(series_name="FXUSDCAD", start_date="2024-01-01", end_date="2024-01-10")

    sent = route.calls.last.request
    assert "start_date=2024-01-01" in str(sent.url)
    assert "end_date=2024-01-10" in str(sent.url)


@respx.mock
def test_boc_fetch_raises_parse_error_on_non_dict_body() -> None:
    """A 200 whose body is not a JSON object → ParseError (not a crash)."""
    respx.get("https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json").mock(
        return_value=httpx.Response(200, json=["unexpected", "list"])
    )

    with pytest.raises(ParseError):
        boc_fetch(series_name="FXUSDCAD")


@respx.mock
def test_boc_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://www.bankofcanada.ca/valet/observations/XX/json").mock(
        return_value=httpx.Response(200, json={"seriesDetail": {"XX": {"label": "x"}}, "observations": []})
    )

    with pytest.raises(EmptyDataError):
        boc_fetch(series_name="XX")


def test_fetch_rejects_empty_series_name() -> None:
    """Blank ``series_name`` is rejected inline before any network call."""
    with pytest.raises(InvalidParameterError):
        boc_fetch(series_name="   ")


def test_fetch_rejects_empty_group_name() -> None:
    """A bare ``group:`` with no name is rejected inline."""
    with pytest.raises(InvalidParameterError):
        boc_fetch(series_name="group:")


def test_fetch_rejects_oversized_series_url() -> None:
    """Valet 302-redirects observations requests whose URL exceeds ~4 KB. Too
    many/long comma-joined names are rejected pre-network with an actionable
    error (split or use group:) rather than an opaque redirect → ParseError.
    No respx route is registered — the guard must fire before any network call.
    """
    long_names = ",".join(f"SERIESNAME{i:05d}" for i in range(300))  # ~4,500 chars
    with pytest.raises(InvalidParameterError) as exc:
        boc_fetch(series_name=long_names)
    msg = str(exc.value).lower()
    assert "group:" in msg  # guidance to split or use a panel
    assert "bytes" in msg


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
    for group_name in groups_payload.get("groups") or {}:
        details = group_membership.get(
            group_name,
            {"name": group_name, "groupSeries": {}},
        )
        respx.get(f"https://www.bankofcanada.ca/valet/groups/{group_name}/json").mock(
            return_value=httpx.Response(200, json={"groupDetails": details})
        )


@respx.mock
def test_enumerate_boc_emits_one_row_per_series_with_description_and_source() -> None:
    """The upstream ``description`` field must populate metadata, and every row must
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

    result = enumerate_boc()
    df = _enumerate_dataframe(result)

    # Per-series rows are emitted alongside group rows (groups are
    # discoverable as their own catalog entries via the ``group:`` prefix).
    series_rows = df[df["entity_type"] == "series"]
    assert set(series_rows["series_name"]) == {"FXUSDCAD", "V39079"}

    fxusdcad = df[df["series_name"] == "FXUSDCAD"].iloc[0]
    assert fxusdcad["title"] == "USD/CAD"
    # ``description`` carries the upstream description text; ``group``
    # carries the upstream group identifier (asserted separately below).
    assert fxusdcad["description"] == "US dollar to Canadian dollar daily exchange rate"
    assert fxusdcad["source"] == "valet"
    assert fxusdcad["entity_type"] == "series"

    # Every row carries source='valet' — future-proofing for a parallel
    # data source so dispatch is already wired.
    assert set(df["source"]) == {"valet"}


@respx.mock
def test_enumerate_boc_group_metadata_is_group_id_not_description() -> None:
    """The ``group`` column carries the upstream group identifier (e.g.
    ``FX_RATES_DAILY``), not the upstream description text. The
    description goes in the ``description`` column."""
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

    df = _enumerate_dataframe(enumerate_boc())
    row = df[df["series_name"] == "FXUSDCAD"].iloc[0]

    assert row["group"] == "FX_RATES_DAILY"
    assert row["group_label"] == "Daily exchange rates"
    # The upstream description text must NOT leak into the group column.
    assert "Canadian dollar" not in row["group"]


@respx.mock
def test_enumerate_boc_series_with_no_group_membership_has_empty_group() -> None:
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

    df = _enumerate_dataframe(enumerate_boc())
    row = df[df["series_name"] == "ORPHAN_SERIES"].iloc[0]
    assert row["group"] == ""
    assert row["group_label"] == ""
    # Description metadata is still populated even when group membership is missing.
    assert row["description"] == "A series in no group at all"


@respx.mock
def test_enumerate_boc_keeps_first_group_when_series_in_multiple_groups() -> None:
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

    df = _enumerate_dataframe(enumerate_boc())
    row = df[df["series_name"] == "FXUSDCAD"].iloc[0]
    assert row["group"] == "FX_RATES_DAILY"
    assert row["group_label"] == "Daily exchange rates"


@respx.mock
def test_enumerate_boc_tolerates_failing_group_endpoint() -> None:
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
            json={"groups": {"FLAKY_GROUP": {"label": "Temporarily flaky group"}}},
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/groups/FLAKY_GROUP/json").mock(
        return_value=httpx.Response(503)
    )

    df = _enumerate_dataframe(enumerate_boc())
    # Series rows: one (FXUSDCAD), no group membership (the fan-out failed).
    series_rows = df[df["entity_type"] == "series"]
    assert set(series_rows["series_name"]) == {"FXUSDCAD"}
    row = series_rows.iloc[0]
    assert row["group"] == ""
    assert row["description"] == "USD to CAD"
    # The group is KEPT — a 5xx is transient, not a retirement.
    group_rows = df[df["entity_type"] == "group"]
    assert set(group_rows["series_name"]) == {"group:FLAKY_GROUP"}


@respx.mock
def test_enumerate_boc_prunes_retired_group_on_404() -> None:
    """A group whose /groups/{name}/json returns **404** is retired — BoC leaves
    ~29 dated one-off panels in /lists/groups that 404 on both the detail and
    observations endpoints. The membership fan-out doubles as a liveness probe:
    a 404 group is PRUNED so the catalog never offers an unfetchable panel. A
    live group alongside it is still emitted."""
    respx.get("https://www.bankofcanada.ca/valet/lists/series/json").mock(
        return_value=httpx.Response(
            200,
            json={"series": {"FXUSDCAD": {"label": "USD/CAD", "description": "USD to CAD"}}},
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/lists/groups/json").mock(
        return_value=httpx.Response(
            200,
            json={
                "groups": {
                    "FX_RATES_DAILY": {"label": "Daily exchange rates"},
                    "EXP_20220303": {"label": "Retired one-off panel"},
                }
            },
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/groups/FX_RATES_DAILY/json").mock(
        return_value=httpx.Response(
            200,
            json={"groupDetails": {"name": "FX_RATES_DAILY", "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}}}},
        )
    )
    respx.get("https://www.bankofcanada.ca/valet/groups/EXP_20220303/json").mock(
        return_value=httpx.Response(404)
    )

    df = _enumerate_dataframe(enumerate_boc())
    group_rows = df[df["entity_type"] == "group"]
    # The retired (404) group is pruned; only the live group is catalogued.
    assert set(group_rows["series_name"]) == {"group:FX_RATES_DAILY"}
    assert "group:EXP_20220303" not in set(df["series_name"])
    # The live group still resolved its membership.
    fx = df[df["series_name"] == "FXUSDCAD"].iloc[0]
    assert fx["group"] == "FX_RATES_DAILY"


@respx.mock
def test_enumerate_boc_emits_one_row_per_group_with_group_prefix_key() -> None:
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

    df = _enumerate_dataframe(enumerate_boc())

    group_rows = df[df["entity_type"] == "group"]
    assert set(group_rows["series_name"]) == {
        "group:FX_RATES_DAILY",
        "group:A4_FUNDS_CONSUMER",
    }
    fx_group = df[df["series_name"] == "group:FX_RATES_DAILY"].iloc[0]
    assert fx_group["title"] == "Daily exchange rates"
    # Group description text is the most useful retrieval signal at the
    # group level — units, frequency, methodology hints.
    assert "Daily average exchange rates" in fx_group["description"]
    assert fx_group["source"] == "valet"
    assert fx_group["group"] == "FX_RATES_DAILY"
    assert fx_group["group_label"] == "Daily exchange rates"

    # The economically-meaningful "Month-end, Millions of dollars" hint is
    # only available at the group level — no series carries it.
    funds_group = df[df["series_name"] == "group:A4_FUNDS_CONSUMER"].iloc[0]
    assert funds_group["description"] == "Month-end, Millions of dollars"


@respx.mock
def test_enumerate_boc_emits_columns_required_for_catalog_entries() -> None:
    """The Result returned by the enumerator must carry catalog entries
    with exactly one KEY (series_name), one TITLE (title), and METADATA
    columns for source/group/group_label.
    """
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

    result = enumerate_boc()
    entries = Result(data=result.data, output_spec=BOC_ENUMERATE_OUTPUT).to_entities()
    # One series row plus one group row. Groups are catalogued as their
    # own discoverable entities so agents can find them via group-level
    # description text.
    assert len(entries) == 2
    by_code = {e.code: e for e in entries}
    series_entry = by_code["FXUSDCAD"]
    assert series_entry.namespace == "boc"
    assert series_entry.title == "USD/CAD"
    assert series_entry.metadata.get("description") == "US dollar to Canadian dollar daily exchange rate"
    assert series_entry.metadata.get("source") == "valet"
    assert series_entry.metadata.get("entity_type") == "series"
    assert series_entry.metadata.get("group") == "FX_RATES_DAILY"
    assert series_entry.metadata.get("group_label") == "Daily exchange rates"

    group_entry = by_code["group:FX_RATES_DAILY"]
    assert group_entry.namespace == "boc"
    assert group_entry.title == "Daily exchange rates"
    assert group_entry.metadata.get("entity_type") == "group"
    assert group_entry.metadata.get("group") == "FX_RATES_DAILY"


@respx.mock
def test_enumerate_boc_columns_exactly_match_declared_schema() -> None:
    """The emitted DataFrame columns exactly match the declared @enumerator
    schema (enumerators drop unmapped columns then require an exact match)."""
    _mock_enumerate_endpoints(
        series_payload={"series": {"FXUSDCAD": {"label": "USD/CAD", "description": "USD to CAD"}}},
        groups_payload={"groups": {"FX_RATES_DAILY": {"label": "Daily exchange rates"}}},
        group_membership={
            "FX_RATES_DAILY": {
                "name": "FX_RATES_DAILY",
                "groupSeries": {"FXUSDCAD": {"label": "USD/CAD"}},
            },
        },
    )

    df = _enumerate_dataframe(enumerate_boc())
    assert list(df.columns) == [c.name for c in BOC_ENUMERATE_OUTPUT.columns]


@respx.mock
def test_enumerate_boc_maps_http_error_on_failed_list_endpoint() -> None:
    """A non-200 on a list endpoint surfaces a typed ProviderError (not raw httpx)."""
    from parsimony.errors import ProviderError

    respx.get("https://www.bankofcanada.ca/valet/lists/series/json").mock(return_value=httpx.Response(503))

    with pytest.raises(ProviderError):
        enumerate_boc()
