"""Offline (respx-mocked) tests for the EIA connectors.

Covers the five verbs (``eia_fetch``, ``eia_fetch_series``, ``eia_facets``,
``enumerate_eia``, ``eia_search``), the offset-pagination assembly, the
row-count ceiling, the 400-message-preserving InvalidParameterError, the
route-tree enumerate fan-out, a parametrized no-key fast-fail over every keyed
verb (with a count-guard so dropping one breaks CI), and secret stripping.
Mocks are hand-authored from the live API shape, never recorded cassettes.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx
from parsimony.errors import (
    EmptyDataError,
    InvalidParameterError,
    RateLimitError,
    UnauthorizedError,
)

from parsimony_eia import CONNECTORS
from parsimony_eia.connectors.enumerate import enumerate_eia
from parsimony_eia.connectors.fetch import eia_facets, eia_fetch, eia_fetch_series

_KEY = "live-looking-eia-xyz"
_DATA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data"
_WTI_DESC = "Cushing, OK WTI Spot Price FOB"


def _data_response(rows: list[dict[str, Any]], total: int, description: str = "Spot Prices") -> dict[str, Any]:
    return {"response": {"description": description, "total": str(total), "data": rows}}


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"eia_fetch", "eia_fetch_series", "eia_facets", "enumerate_eia", "eia_search"}


# ---------------------------------------------------------------------------
# eia_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_eia_fetch_returns_rows() -> None:
    respx.get(_DATA_URL).mock(
        return_value=httpx.Response(
            200,
            json=_data_response(
                [
                    {"period": "2026-03", "value": 78.5, "duoarea": "NUS", "product": "EPCBRENT"},
                    {"period": "2026-02", "value": 77.0, "duoarea": "NUS", "product": "EPCBRENT"},
                ],
                total=2,
            ),
        )
    )
    result = eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt")

    assert result.provenance.source == "eia_fetch"
    df = result.raw
    assert len(df) == 2
    assert df.iloc[0]["title"] == "Spot Prices"
    assert (df["route"] == "petroleum/pri/spt").all()
    assert df["value"].notna().all()
    # String facet metadata is NOT coerced to NaN (only the measure is).
    assert (df["product"] == "EPCBRENT").all()


@respx.mock
def test_eia_fetch_paginates_across_pages() -> None:
    """total=3 but page1 returns 2 rows → a second page (offset=2) is fetched."""

    def _router(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(
                200,
                json=_data_response(
                    [{"period": "2026-03", "value": 3.0}, {"period": "2026-02", "value": 2.0}], total=3
                ),
            )
        return httpx.Response(200, json=_data_response([{"period": "2026-01", "value": 1.0}], total=3))

    respx.get(_DATA_URL).mock(side_effect=_router)
    df = eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt").raw
    # All three rows assembled across the two pages (5000-cap pagination).
    assert len(df) == 3
    assert sorted(df["value"].tolist()) == [1.0, 2.0, 3.0]


@respx.mock
def test_eia_fetch_dedups_boundary_duplicate() -> None:
    """An offset-boundary duplicate row is dropped on the natural key."""

    def _router(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        if offset == 0:
            return httpx.Response(
                200,
                json=_data_response(
                    [
                        {"period": "2026-03", "series": "A", "value": 3.0},
                        {"period": "2026-02", "series": "A", "value": 2.0},
                    ],
                    total=3,
                ),
            )
        # page 2 repeats the last row of page 1 (boundary dup) + one new row
        return httpx.Response(
            200,
            json=_data_response(
                [
                    {"period": "2026-02", "series": "A", "value": 2.0},
                    {"period": "2026-01", "series": "A", "value": 1.0},
                ],
                total=3,
            ),
        )

    respx.get(_DATA_URL).mock(side_effect=_router)
    df = eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt").raw
    assert len(df) == 3  # the duplicate (period 2026-02, series A) collapsed to one


@respx.mock
def test_eia_fetch_rejects_over_ceiling() -> None:
    respx.get(_DATA_URL).mock(
        return_value=httpx.Response(200, json=_data_response([{"period": "2026", "value": 1.0}], total=18_000_000))
    )
    with pytest.raises(InvalidParameterError, match="ceiling"):
        eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt")


@respx.mock
def test_eia_fetch_non_value_measure_normalized() -> None:
    url = "https://api.eia.gov/v2/electricity/retail-sales/data"
    respx.get(url).mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "description": "Retail Sales",
                    "total": "1",
                    "data": [{"period": "2020", "price": 10.5, "stateid": "CO", "price-units": "cents/kWh"}],
                }
            },
        )
    )
    df = eia_fetch.bind(api_key=_KEY)(route="electricity/retail-sales", measure="price").raw
    assert "value" in df.columns
    assert df.iloc[0]["value"] == 10.5
    assert (df["stateid"] == "CO").all()


@respx.mock
def test_eia_fetch_sends_facet_filters() -> None:
    captured: dict[str, list[str]] = {}

    def _router(request: httpx.Request) -> httpx.Response:
        captured["product"] = request.url.params.get_list("facets[product][]")
        captured["duoarea"] = request.url.params.get_list("facets[duoarea][]")
        return httpx.Response(200, json=_data_response([{"period": "2026-03", "value": 1.0}], total=1))

    respx.get(_DATA_URL).mock(side_effect=_router)
    eia_fetch.bind(api_key=_KEY)(
        route="petroleum/pri/spt", facets={"product": ["EPCBRENT", "EPD2DC"], "duoarea": "NUS"}
    )
    assert captured["product"] == ["EPCBRENT", "EPD2DC"]
    assert captured["duoarea"] == ["NUS"]


@respx.mock
def test_eia_fetch_maps_400_to_invalid_parameter_with_message() -> None:
    respx.get(_DATA_URL).mock(
        return_value=httpx.Response(
            400, json={"error": "Invalid data 'bogus' provided. The only valid data are 'value'.", "code": 400}
        )
    )
    with pytest.raises(InvalidParameterError) as exc_info:
        eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt", measure="bogus")
    assert "valid data" in str(exc_info.value)
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_eia_fetch_maps_401_without_leaking_key() -> None:
    respx.get(_DATA_URL).mock(return_value=httpx.Response(401, text="invalid api key"))
    with pytest.raises(UnauthorizedError) as exc_info:
        eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt")
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_eia_fetch_maps_429_without_leaking_key() -> None:
    respx.get(_DATA_URL).mock(return_value=httpx.Response(429, headers={"Retry-After": "30"}, text="too many"))
    with pytest.raises(RateLimitError) as exc_info:
        eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt")
    assert _KEY not in str(exc_info.value)


@respx.mock
def test_eia_fetch_raises_empty_data_when_no_records() -> None:
    respx.get(_DATA_URL).mock(return_value=httpx.Response(200, json=_data_response([], total=0)))
    with pytest.raises(EmptyDataError):
        eia_fetch.bind(api_key=_KEY)(route="petroleum/pri/spt")


def test_eia_fetch_rejects_empty_route() -> None:
    with pytest.raises(InvalidParameterError, match="route"):
        eia_fetch.bind(api_key=_KEY)(route="   ")


# ---------------------------------------------------------------------------
# eia_fetch_series (legacy /v2/seriesid/{id})
# ---------------------------------------------------------------------------


@respx.mock
def test_eia_fetch_series_returns_rows_with_series_title() -> None:
    respx.get("https://api.eia.gov/v2/seriesid/PET.RWTC.D").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "description": "EIA petroleum survey data",
                    "total": "2",
                    "data": [
                        {"period": "2026-06-01", "series": "RWTC", "series-description": _WTI_DESC, "value": 95.9},
                        {"period": "2026-05-29", "series": "RWTC", "series-description": _WTI_DESC, "value": 94.1},
                    ],
                }
            },
        )
    )
    result = eia_fetch_series.bind(api_key=_KEY)(series_id="PET.RWTC.D")
    assert result.provenance.source == "eia_fetch_series"
    df = result.raw
    assert len(df) == 2
    assert (df["series_id"] == "PET.RWTC.D").all()
    # Title prefers the specific series-description over the generic dataset blurb.
    assert df.iloc[0]["title"] == _WTI_DESC
    assert df["value"].notna().all()


def test_eia_fetch_series_rejects_empty_id() -> None:
    with pytest.raises(InvalidParameterError, match="series_id"):
        eia_fetch_series.bind(api_key=_KEY)(series_id="  ")


# ---------------------------------------------------------------------------
# eia_facets
# ---------------------------------------------------------------------------


@respx.mock
def test_eia_facets_lists_values() -> None:
    respx.get("https://api.eia.gov/v2/petroleum/pri/spt/facet/product").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "facets": [
                        {"id": "EPCBRENT", "name": "UK Brent Crude Oil"},
                        {"id": "EPD2DC", "name": "Carb Diesel"},
                    ]
                }
            },
        )
    )
    df = eia_facets.bind(api_key=_KEY)(route="petroleum/pri/spt", facet="product").raw
    assert list(df.columns) == ["facet_value", "name", "facet", "route"]
    assert set(df["facet_value"]) == {"EPCBRENT", "EPD2DC"}
    assert (df["facet"] == "product").all()


@respx.mock
def test_eia_facets_empty_raises() -> None:
    respx.get("https://api.eia.gov/v2/x/y/facet/z").mock(
        return_value=httpx.Response(200, json={"response": {"facets": []}})
    )
    with pytest.raises(EmptyDataError):
        eia_facets.bind(api_key=_KEY)(route="x/y", facet="z")


# ---------------------------------------------------------------------------
# enumerate_eia (route-tree fan-out)
# ---------------------------------------------------------------------------


@respx.mock
def test_enumerate_eia_walks_tree_to_leaf_datasets() -> None:
    # root -> one category "coal" -> a parent "coal/agg" -> a leaf dataset
    respx.get("https://api.eia.gov/v2/").mock(
        return_value=httpx.Response(200, json={"response": {"routes": [{"id": "coal"}]}})
    )
    respx.get("https://api.eia.gov/v2/coal").mock(
        return_value=httpx.Response(200, json={"response": {"routes": [{"id": "aggregate-production"}]}})
    )
    respx.get("https://api.eia.gov/v2/coal/aggregate-production").mock(
        return_value=httpx.Response(
            200,
            json={
                "response": {
                    "id": "aggregate-production",
                    "name": "Aggregate Production",
                    "description": "Coal production aggregates.",
                    "frequency": [{"id": "annual"}],
                    "facets": [{"id": "coalRankId", "description": "Coal rank"}],
                    "data": {"production": {"units": "thousand short tons"}},
                    "startPeriod": "2001",
                    "endPeriod": "2024",
                    "defaultFrequency": "annual",
                }
            },
        )
    )

    result = enumerate_eia.bind(api_key=_KEY)()
    df = result.raw
    # Exact column match against the declared enumerate schema.
    from parsimony_eia.outputs import ENUMERATE_COLUMNS

    assert list(df.columns) == list(ENUMERATE_COLUMNS)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["code"] == "coal/aggregate-production"
    assert row["title"] == "Aggregate Production"
    assert row["category"] == "coal"
    assert row["measures"] == "production"
    assert row["facets"] == "coalRankId"
    assert row["frequencies"] == "annual"
    # The manifest is folded into the searchable description.
    assert "production" in row["description"]
    assert "coalRankId" in row["description"]


# ---------------------------------------------------------------------------
# No-key fast-fail over every keyed verb (count-guarded)
# ---------------------------------------------------------------------------

_KEYED_CALLS = [
    (eia_fetch, {"route": "petroleum/pri/spt"}),
    (eia_fetch_series, {"series_id": "PET.RWTC.D"}),
    (eia_facets, {"route": "petroleum/pri/spt", "facet": "product"}),
    (enumerate_eia, {}),
]


def test_keyed_verb_count_guard() -> None:
    # CONNECTORS = 4 keyed verbs + eia_search (catalog-based, not keyed).
    assert len(_KEYED_CALLS) == 4
    assert len(CONNECTORS) == 5


@pytest.mark.parametrize("connector,kwargs", _KEYED_CALLS)
def test_verb_fast_fails_without_key(connector: Any, kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(UnauthorizedError) as exc_info:
        connector(**kwargs)
    assert exc_info.value.env_var == "EIA_API_KEY"
    assert exc_info.value.provider == "eia"
