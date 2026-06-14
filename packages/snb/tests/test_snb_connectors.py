"""Offline tests for the SNB connectors (respx-mocked from the live API shapes).

SNB is keyless; the 401/429 credential contract does not apply. These tests cover
the sitemap parse, CSV parse, publication/warehouse fetch routing, getCubeInfo
title enrichment (+ best-effort fallback), the dimensions → series cartesian
product, and the live-sitemap enumerator (bounded by monkeypatching the
``_list_cubes`` seam).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError, ProviderError

from parsimony_snb import CONNECTORS, parsing, snb_fetch
from parsimony_snb import _http as http_mod
from parsimony_snb.connectors import enumerate as enum_mod
from parsimony_snb.connectors.enumerate import enumerate_snb
from parsimony_snb.outputs import _ENUMERATE_COLUMNS

# A real-shaped publication cube CSV: BOM + preamble + blank line + long-format
# header + data rows (mirrors a live `rendoblim` download).
_PUB_CSV = (
    '﻿"CubeId";"rendoblim"\r\n'
    '"PublishingDate";"2025-09-01 14:29"\r\n'
    "\r\n"
    '"Date";"D0";"Value"\r\n'
    '"2026-01";"10J";"0.83"\r\n'
    '"2026-02";"10J";"0.86"\r\n'
)

# A real-shaped warehouse cube CSV (the preamble echoes the portal id with `@`).
_WH_CSV = (
    '﻿"CubeId";"BSTA@SNB.AUR_U.ODF"\r\n'
    '"PublishingDate";"2025-09-25 09:48"\r\n'
    "\r\n"
    '"Date";"KONSOLIDIERUNGSSTUFE";"Value"\r\n'
    '"2024";"U";"214274863.15"\r\n'
)

_RENDOBLIM_DIMS: dict = {
    "cubeId": "rendoblim",
    "dimensions": [
        {
            "id": "D0",
            "name": "Overview",
            "dimensionItems": [
                {
                    "id": "D0_0",
                    "name": "CHF Swiss Confederation bond issues",
                    "dimensionItems": [
                        {"id": "1J", "name": "1 year"},
                        {"id": "10J", "name": "10 years"},
                        {"id": "30J", "name": "30 years"},
                    ],
                }
            ],
        }
    ],
}

_DEVKUM_DIMS: dict = {
    "cubeId": "devkum",
    "dimensions": [
        {
            "id": "D0",
            "name": "Monthly average/End of month",
            "dimensionItems": [
                {"id": "M0", "name": "Monthly average"},
                {"id": "M1", "name": "End of month"},
            ],
        },
        {
            "id": "D1",
            "name": "Currency",
            "dimensionItems": [
                {"id": "D1_0", "name": "Europe", "dimensionItems": [{"id": "EUR1", "name": "EUR 1"}]},
                {"id": "D1_1", "name": "America", "dimensionItems": [{"id": "USD1", "name": "USD 1"}]},
            ],
        },
    ],
}

_CUBE_INFO = {
    "rendoblim": {
        "title": "Yields on bond issues – Month",
        "publishingTitle": "Interest rates and exchange rates",
        "unit": "In percent",
        "frequencySpecification": "End of month",
    },
    "BSTA@SNB.AUR_U.ODF": {
        "title": "Outstanding derivative financial instruments",
        "publishingTitle": "Annual banking statistics",
    },
}


def _cube_info_response(request: httpx.Request) -> httpx.Response:
    cid = request.url.params.get("cubeId", "")
    return httpx.Response(200, json=_CUBE_INFO.get(cid, {}))


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    assert {c.name for c in CONNECTORS} == {"snb_fetch", "enumerate_snb", "snb_search"}


# ---------------------------------------------------------------------------
# Cube-id routing helpers
# ---------------------------------------------------------------------------


def test_warehouse_id_detection_and_transform() -> None:
    assert http_mod.is_warehouse_id("BSTA@SNB.AUR_U.ODF") is True
    assert http_mod.is_warehouse_id("rendoblim") is False
    # The @→. transform is the verified warehouse fetch-path id.
    assert http_mod.warehouse_api_id("BSTA@SNB.AUR_U.ODF") == "BSTA.SNB.AUR_U.ODF"


def test_cube_paths_route_publication_vs_warehouse() -> None:
    assert http_mod.cube_data_path("rendoblim", lang="en") == "/api/cube/rendoblim/data/csv/en"
    assert (
        http_mod.cube_data_path("BSTA@SNB.AUR_U.ODF", lang="en")
        == "/api/warehouse/cube/BSTA.SNB.AUR_U.ODF/data/csv/en"
    )
    assert http_mod.cube_dimensions_path("rendoblim", lang="en") == "/api/cube/rendoblim/dimensions/en"
    assert (
        http_mod.cube_dimensions_path("BSTA@SNB.AUR_U.ODF", lang="en")
        == "/api/warehouse/cube/BSTA.SNB.AUR_U.ODF/dimensions/en"
    )


# ---------------------------------------------------------------------------
# Sitemap parse
# ---------------------------------------------------------------------------

_SITEMAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://data.snb.ch/en/topics/ziredev/cube/rendoblim</loc></url>
  <url><loc>https://data.snb.ch/de/topics/ziredev/cube/rendoblim</loc></url>
  <url><loc>https://data.snb.ch/en/topics/snb/cube/snbmonagg</loc></url>
  <url><loc>https://data.snb.ch/en/warehouse/BSTA/cube/BSTA@SNB.AUR_U.ODF</loc></url>
  <url><loc>https://data.snb.ch/en/warehouse/ZAST/cube/ZAST@SNB.IEA.AMNEC.1.11.10000</loc></url>
  <url><loc>https://data.snb.ch/en/warehouse/BSTA/facets</loc></url>
  <url><loc>https://data.snb.ch/en/help_api</loc></url>
</urlset>"""


def test_parse_sitemap_splits_publication_and_warehouse() -> None:
    cubes = parsing.parse_sitemap(_SITEMAP_XML)
    by_id = {cid: (kind, group) for cid, kind, group in cubes}
    # Two publication cubes (de duplicate dropped), two warehouse cubes, no /facets or /help.
    assert by_id["rendoblim"] == ("publication", "ziredev")
    assert by_id["snbmonagg"] == ("publication", "snb")
    assert by_id["BSTA@SNB.AUR_U.ODF"] == ("warehouse", "BSTA")
    assert by_id["ZAST@SNB.IEA.AMNEC.1.11.10000"] == ("warehouse", "ZAST")
    assert len(cubes) == 4


# ---------------------------------------------------------------------------
# CSV parse
# ---------------------------------------------------------------------------


def test_parse_snb_csv_coerces_value_keeps_dimension_strings() -> None:
    df = parsing.parse_snb_csv(_PUB_CSV, "rendoblim")
    assert df["Value"].dtype.kind == "f"
    assert df["Value"].tolist() == [0.83, 0.86]
    assert df["D0"].dtype == object
    assert set(df["D0"]) == {"10J"}


def test_parse_snb_csv_raises_on_unparseable_body() -> None:
    with pytest.raises(ParseError):
        parsing.parse_snb_csv('{"message": "not found"}', "rendoblim")


def test_parse_snb_csv_returns_empty_frame_on_blank_body() -> None:
    assert parsing.parse_snb_csv("", "rendoblim").empty


def test_normalize_frequency() -> None:
    assert parsing.normalize_frequency("End of month") == "Monthly"
    assert parsing.normalize_frequency("Daily") == "Daily"
    assert parsing.normalize_frequency("Quarter") == "Quarterly"
    assert parsing.normalize_frequency(None) == "Unknown"
    assert parsing.normalize_frequency("Irregular") == "Unknown"


# ---------------------------------------------------------------------------
# snb_fetch — publication + warehouse
# ---------------------------------------------------------------------------


@respx.mock
def test_snb_fetch_publication_parses_csv_and_resolves_title() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text=_PUB_CSV)
    )
    respx.get(url__startswith="https://data.snb.ch/json/table/getCubeInfo").mock(
        side_effect=_cube_info_response
    )

    result = snb_fetch(cube_id="rendoblim")
    assert result.provenance.source == "snb_fetch"
    df = result.data
    assert set(df["cube_id"]) == {"rendoblim"}
    assert df.iloc[0]["title"] == "Yields on bond issues – Month"  # from getCubeInfo
    assert df["Value"].dtype.kind == "f"
    assert set(df["D0"]) == {"10J"}
    assert df["date"].dtype.kind == "M"


@respx.mock
def test_snb_fetch_warehouse_routes_to_warehouse_path() -> None:
    """A warehouse cube_id (with @) routes to /api/warehouse/cube/{@→.}/..."""
    wh_route = respx.get(
        "https://data.snb.ch/api/warehouse/cube/BSTA.SNB.AUR_U.ODF/data/csv/en"
    ).mock(return_value=httpx.Response(200, text=_WH_CSV))
    # The publication path must NOT be hit for a warehouse id.
    pub_route = respx.get(
        "https://data.snb.ch/api/cube/BSTA@SNB.AUR_U.ODF/data/csv/en"
    ).mock(return_value=httpx.Response(500))
    respx.get(url__startswith="https://data.snb.ch/json/table/getCubeInfo").mock(
        side_effect=_cube_info_response
    )

    df = snb_fetch(cube_id="BSTA@SNB.AUR_U.ODF").data
    assert wh_route.called
    assert not pub_route.called
    assert set(df["cube_id"]) == {"BSTA@SNB.AUR_U.ODF"}
    assert df.iloc[0]["title"] == "Outstanding derivative financial instruments"
    assert "KONSOLIDIERUNGSSTUFE" in df.columns
    assert df["Value"].dtype.kind == "f"


@respx.mock
def test_snb_fetch_title_falls_back_to_cube_id_when_getcubeinfo_fails() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text=_PUB_CSV)
    )
    respx.get(url__startswith="https://data.snb.ch/json/table/getCubeInfo").mock(
        return_value=httpx.Response(500, text="boom")
    )
    df = snb_fetch(cube_id="rendoblim").data
    assert set(df["title"]) == {"rendoblim"}  # graceful degradation, not a crash


@respx.mock
def test_snb_fetch_raises_empty_data_on_empty_csv() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text="")
    )
    with pytest.raises(EmptyDataError):
        snb_fetch(cube_id="rendoblim")


@respx.mock
def test_snb_fetch_raises_parse_error_on_html_error_page() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text="<html><body>Service unavailable</body></html>")
    )
    with pytest.raises(ParseError):
        snb_fetch(cube_id="rendoblim")


@respx.mock
def test_snb_fetch_maps_http_error_to_provider_error() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(500)
    )
    with pytest.raises(ProviderError):
        snb_fetch(cube_id="rendoblim")


def test_fetch_rejects_empty_cube_id() -> None:
    with pytest.raises(InvalidParameterError):
        snb_fetch(cube_id="   ")


# ---------------------------------------------------------------------------
# series_from_dimensions — compound code + cartesian product
# ---------------------------------------------------------------------------


def _series(cube_id: str, cube_title: str, dims: dict | None) -> list[dict[str, str]]:
    return parsing.series_from_dimensions(
        cube_id,
        cube_title=cube_title,
        dimensions_payload=dims,
        source="snb_data_portal",
        category="Interest rates",
        frequency="Monthly",
        unit="In percent",
    )


def test_series_from_dimensions_single_dim_cartesian() -> None:
    rows = _series("rendoblim", "Yields on bond issues", _RENDOBLIM_DIMS)
    assert {r["code"] for r in rows} == {"rendoblim#1J", "rendoblim#10J", "rendoblim#30J"}
    ten_year = next(r for r in rows if r["code"] == "rendoblim#10J")
    assert ten_year["series_key"] == "10J"
    assert "10 years" in ten_year["title"]
    assert "10 years" in ten_year["description"]
    assert ten_year["unit"] == "In percent"


def test_series_from_dimensions_multi_dim_cartesian() -> None:
    rows = _series("devkum", "FX monthly", _DEVKUM_DIMS)
    assert {r["code"] for r in rows} == {
        "devkum#M0.EUR1",
        "devkum#M0.USD1",
        "devkum#M1.EUR1",
        "devkum#M1.USD1",
    }
    usd_eom = next(r for r in rows if r["code"] == "devkum#M1.USD1")
    assert "USD 1" in usd_eom["dimension_path"]
    assert "End of month" in usd_eom["dimension_path"]


def test_series_from_dimensions_empty_falls_back_to_cube_row() -> None:
    rows = _series("foo", "Foo cube", {"cubeId": "foo", "dimensions": []})
    assert len(rows) == 1 and rows[0]["code"] == "foo#"


def test_series_from_dimensions_handles_none_payload() -> None:
    rows = _series("foo", "Foo cube", None)
    assert len(rows) == 1
    assert rows[0]["code"] == "foo#"


def test_series_from_dimensions_collapses_oversized_cube() -> None:
    cap = parsing._MAX_SERIES_PER_CUBE
    big = {
        "cubeId": "huge",
        "dimensions": [
            {"id": "D0", "name": "Big", "dimensionItems": [{"id": f"X{i}", "name": str(i)} for i in range(cap + 5)]}
        ],
    }
    rows = _series("huge", "Mega cube", big)
    assert len(rows) == 1 and rows[0]["code"] == "huge#" and rows[0]["series_key"] == ""


def test_cube_level_row_for_warehouse() -> None:
    row = parsing.cube_level_row(
        "BSTA@SNB.AUR_U.ODF",
        source="snb_warehouse",
        title="Outstanding derivative financial instruments",
        category="Annual banking statistics",
        frequency="Unknown",
        unit="",
    )
    assert row["code"] == "BSTA@SNB.AUR_U.ODF#"
    assert row["source"] == "snb_warehouse"
    assert "derivative" in row["description"]


# ---------------------------------------------------------------------------
# enumerate_snb — live sitemap discovery, bounded by the _list_cubes seam
# ---------------------------------------------------------------------------


def _fake_list_cubes() -> list[tuple[str, str, str]]:
    return [
        ("rendoblim", "publication", "ziredev"),
        ("BSTA@SNB.AUR_U.ODF", "warehouse", "BSTA"),
    ]


@respx.mock
def test_enumerate_snb_publication_and_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(enum_mod, "_list_cubes", _fake_list_cubes)
    respx.get(url__startswith="https://data.snb.ch/json/table/getCubeInfo").mock(
        side_effect=_cube_info_response
    )
    respx.get("https://data.snb.ch/api/cube/rendoblim/dimensions/en").mock(
        return_value=httpx.Response(200, json=_RENDOBLIM_DIMS)
    )

    df = enumerate_snb().data

    # @enumerator enforces the exact declared column set.
    assert list(df.columns) == list(_ENUMERATE_COLUMNS)
    # rendoblim → 3 publication series; warehouse → 1 cube-level row.
    assert set(df["source"]) == {"snb_data_portal", "snb_warehouse"}
    pub = df[df["source"] == "snb_data_portal"]
    wh = df[df["source"] == "snb_warehouse"]
    assert set(pub["code"]) == {"rendoblim#1J", "rendoblim#10J", "rendoblim#30J"}
    assert (pub["frequency"] == "Monthly").all()  # normalized from "End of month"
    assert (pub["category"] == "Interest rates and exchange rates").all()  # publishingTitle
    assert len(wh) == 1
    assert wh.iloc[0]["code"] == "BSTA@SNB.AUR_U.ODF#"
    assert wh.iloc[0]["title"] == "Outstanding derivative financial instruments"


@respx.mock
def test_enumerate_snb_synthesizes_title_when_getcubeinfo_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """getCubeInfo failing must not drop a cube — the title is synthesized."""
    monkeypatch.setattr(enum_mod, "_list_cubes", _fake_list_cubes)
    respx.get(url__startswith="https://data.snb.ch/json/table/getCubeInfo").mock(
        return_value=httpx.Response(500, text="boom")
    )
    respx.get("https://data.snb.ch/api/cube/rendoblim/dimensions/en").mock(
        return_value=httpx.Response(200, json=_RENDOBLIM_DIMS)
    )

    df = enumerate_snb().data
    assert not df.empty
    wh = df[df["source"] == "snb_warehouse"].iloc[0]
    # Synthesized "{cube_id} — {group label}" — never an empty title.
    assert wh["title"].startswith("BSTA@SNB.AUR_U.ODF — ")
    assert df["title"].astype(str).str.len().gt(0).all()


@respx.mock
def test_enumerate_snb_empty_when_sitemap_yields_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    def _empty() -> list[tuple[str, str, str]]:
        return []

    monkeypatch.setattr(enum_mod, "_list_cubes", _empty)
    df = enumerate_snb().data
    assert df.empty
    assert list(df.columns) == list(_ENUMERATE_COLUMNS)
