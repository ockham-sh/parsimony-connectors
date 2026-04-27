"""Happy-path tests for the SNB connectors.

SNB is public-data (no api_key); template 401/429 contract does not apply.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

import parsimony_snb as _snb_module
from parsimony_snb import (
    CONNECTORS,
    SnbEnumerateParams,
    SnbFetchParams,
    _is_measure_series,
    _series_from_dimensions,
    enumerate_snb,
    snb_fetch,
)

_SNB_CSV = (
    "﻿date;value\n"
    "2026-01;108.4\n"
    "2026-02;108.7\n"
)


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"snb_fetch", "enumerate_snb"}


@respx.mock
@pytest.mark.asyncio
async def test_snb_fetch_parses_csv() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text=_SNB_CSV)
    )
    # Dimensions endpoint — returned but not required for the test
    respx.get("https://data.snb.ch/api/cube/rendoblim/dimensions/en").mock(
        return_value=httpx.Response(200, json={"name": "Bond yields"})
    )

    result = await snb_fetch(SnbFetchParams(cube_id="rendoblim"))

    assert result.provenance.source == "snb"
    df = result.data
    assert "cube_id" in df.columns
    assert df.iloc[0]["cube_id"] == "rendoblim"


@respx.mock
@pytest.mark.asyncio
async def test_snb_fetch_raises_empty_data_on_empty_csv() -> None:
    respx.get("https://data.snb.ch/api/cube/rendoblim/data/csv/en").mock(
        return_value=httpx.Response(200, text="")
    )

    with pytest.raises(EmptyDataError):
        await snb_fetch(SnbFetchParams(cube_id="rendoblim"))


def test_fetch_rejects_empty_cube_id() -> None:
    with pytest.raises(ValueError):
        SnbFetchParams(cube_id="   ")


# ---------------------------------------------------------------------------
# _is_measure_series — structural filter
# ---------------------------------------------------------------------------


def test_is_measure_series_accepts_leaf_item() -> None:
    assert _is_measure_series({"id": "10J", "name": "10 years"}) is True


def test_is_measure_series_rejects_grouping_node() -> None:
    grouping = {
        "id": "D1_0",
        "name": "Europe",
        "dimensionItems": [{"id": "EUR1", "name": "EUR 1"}],
    }
    assert _is_measure_series(grouping) is False


def test_is_measure_series_rejects_item_without_id() -> None:
    assert _is_measure_series({"name": "no id"}) is False
    assert _is_measure_series({}) is False


def test_is_measure_series_rejects_non_dict() -> None:
    assert _is_measure_series("foo") is False  # type: ignore[arg-type]
    assert _is_measure_series(None) is False  # type: ignore[arg-type]


def test_is_measure_series_treats_empty_children_as_leaf() -> None:
    # An empty dimensionItems list is not a real branching — treat as leaf.
    assert _is_measure_series({"id": "X", "dimensionItems": []}) is True


# ---------------------------------------------------------------------------
# _series_from_dimensions — compound code + cartesian product
# ---------------------------------------------------------------------------


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
                {
                    "id": "D1_0",
                    "name": "Europe",
                    "dimensionItems": [{"id": "EUR1", "name": "EUR 1"}],
                },
                {
                    "id": "D1_1",
                    "name": "America",
                    "dimensionItems": [{"id": "USD1", "name": "USD 1"}],
                },
            ],
        },
    ],
}


def test_series_from_dimensions_single_dim_cartesian() -> None:
    rows = _series_from_dimensions(
        "rendoblim",
        "Yields on Swiss Confederation bonds",
        _RENDOBLIM_DIMS,
    )
    assert {r["code"] for r in rows} == {
        "rendoblim#1J",
        "rendoblim#10J",
        "rendoblim#30J",
    }
    ten_year = next(r for r in rows if r["code"] == "rendoblim#10J")
    assert ten_year["source"] == "snb_data_portal"
    assert ten_year["cube_id"] == "rendoblim"
    assert ten_year["series_key"] == "10J"
    # Title surfaces both the leaf label and the cube name.
    assert "10 years" in ten_year["title"]
    # Description carries the human-readable dimension breadcrumb so the
    # embedder sees it in semantic_text().
    assert "10 years" in ten_year["description"]


def test_series_from_dimensions_multi_dim_cartesian() -> None:
    rows = _series_from_dimensions("devkum", "FX monthly", _DEVKUM_DIMS)
    codes = {r["code"] for r in rows}
    # 2 (M0,M1) × 2 (EUR1,USD1) = 4 series.
    assert codes == {
        "devkum#M0.EUR1",
        "devkum#M0.USD1",
        "devkum#M1.EUR1",
        "devkum#M1.USD1",
    }
    usd_eom = next(r for r in rows if r["code"] == "devkum#M1.USD1")
    assert usd_eom["dimension_path"]  # populated
    assert "USD 1" in usd_eom["dimension_path"]
    assert "End of month" in usd_eom["dimension_path"]


def test_series_from_dimensions_empty_dimensions_falls_back_to_cube_row() -> None:
    rows = _series_from_dimensions("foo", "Foo cube", {"cubeId": "foo", "dimensions": []})
    assert len(rows) == 1
    assert rows[0]["code"] == "foo#"
    assert rows[0]["source"] == "snb_data_portal"


def test_series_from_dimensions_handles_none_payload() -> None:
    rows = _series_from_dimensions("foo", "Foo cube", None)
    assert len(rows) == 1
    assert rows[0]["code"] == "foo#"


# ---------------------------------------------------------------------------
# enumerate_snb — end-to-end with mocked SNB API
# ---------------------------------------------------------------------------


def _mock_all_known_cubes(*, live: dict[str, dict]) -> None:
    """Mock every cube in the curated list.

    Cubes named in ``live`` get a real dimensions payload + a CSV stub
    so frequency inference picks something concrete; everything else
    returns the SNB error envelope so the enumerator skips them.
    """
    for cid, _ in _snb_module._KNOWN_CUBES:
        if cid in live:
            respx.get(f"https://data.snb.ch/api/cube/{cid}/dimensions/en").mock(
                return_value=httpx.Response(200, json=live[cid]["dimensions"])
            )
            respx.get(f"https://data.snb.ch/api/cube/{cid}/data/csv/en").mock(
                return_value=httpx.Response(200, text=live[cid]["csv"])
            )
        else:
            # SNB's "table not found" envelope is returned with 404 + JSON.
            respx.get(f"https://data.snb.ch/api/cube/{cid}/dimensions/en").mock(
                return_value=httpx.Response(
                    404, json={"message": f"Table {cid} not found"}
                )
            )
            respx.get(f"https://data.snb.ch/api/cube/{cid}/data/csv/en").mock(
                return_value=httpx.Response(404, text="")
            )


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_emits_one_row_per_series_with_compound_code() -> None:
    _mock_all_known_cubes(
        live={
            "rendoblim": {
                "dimensions": _RENDOBLIM_DIMS,
                "csv": '"Date";"D0";"Value"\n"2024-01";"10J";"1.5"\n"2024-02";"10J";"1.6"\n',
            },
            "devkum": {
                "dimensions": _DEVKUM_DIMS,
                "csv": '"Date";"D0";"D1";"Value"\n"2024-01";"M0";"USD1";"0.9"\n',
            },
        }
    )

    result = await enumerate_snb(SnbEnumerateParams())
    df = result.data

    # 3 rendoblim series + 4 devkum series; retired cubes skipped.
    assert len(df) == 7
    assert set(df["cube_id"]) == {"rendoblim", "devkum"}

    codes = set(df["code"])
    assert "rendoblim#10J" in codes
    assert "devkum#M0.USD1" in codes


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_populates_description_for_embedder() -> None:
    """DESCRIPTION column is the catalog's semantic-recall surface — it must
    carry per-series text rich enough for the embedder to differentiate
    yield-curve maturities, currency pairs, etc."""
    _mock_all_known_cubes(
        live={
            "rendoblim": {
                "dimensions": _RENDOBLIM_DIMS,
                "csv": '"Date";"D0";"Value"\n"2024-01";"10J";"1.5"\n',
            },
        }
    )

    df = (await enumerate_snb(SnbEnumerateParams())).data
    ten_year = df[df["code"] == "rendoblim#10J"].iloc[0]
    assert ten_year["description"]
    assert "10 years" in ten_year["description"]
    # Cube context is included so the embedder sees the full identity —
    # the description carries the cube_title verbatim, which the registry
    # sources from the SNB navigation tree (e.g. "Yields on bond issues
    # ‒ 2002 methodology …").
    assert "Yields on bond issues" in ten_year["description"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_emits_source_metadata_for_dispatch() -> None:
    """Every row carries ``source = snb_data_portal`` so an agent dispatching
    off a search hit knows which fetch connector to call without parsing
    the code prefix."""
    _mock_all_known_cubes(
        live={
            "rendoblim": {
                "dimensions": _RENDOBLIM_DIMS,
                "csv": '"Date";"D0";"Value"\n"2024-01";"10J";"1.5"\n',
            },
        }
    )

    df = (await enumerate_snb(SnbEnumerateParams())).data
    assert set(df["source"]) == {"snb_data_portal"}


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_skips_retired_cubes() -> None:
    """Cubes whose ``/dimensions`` returns the SNB error envelope (the
    retired-table case) must not pollute the catalog with rows pointing
    at dead endpoints."""
    _mock_all_known_cubes(live={})

    df = (await enumerate_snb(SnbEnumerateParams())).data
    assert df.empty


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_infers_monthly_frequency_from_csv() -> None:
    _mock_all_known_cubes(
        live={
            "rendoblim": {
                "dimensions": _RENDOBLIM_DIMS,
                # YYYY-MM dates → Monthly inference.
                "csv": '"Date";"D0";"Value"\n"2024-01";"10J";"1.5"\n"2024-02";"10J";"1.6"\n',
            },
        }
    )

    df = (await enumerate_snb(SnbEnumerateParams())).data
    assert (df["frequency"] == "Monthly").all()


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_carries_dimension_path_metadata() -> None:
    """``dimension_path`` is the human-readable breadcrumb the agent can
    surface in tool-call summaries; verify it's threaded through end-to-end."""
    _mock_all_known_cubes(
        live={
            "devkum": {
                "dimensions": _DEVKUM_DIMS,
                "csv": '"Date";"D0";"D1";"Value"\n"2024-01";"M0";"USD1";"0.9"\n',
            },
        }
    )
    df = (await enumerate_snb(SnbEnumerateParams())).data
    usd = df[df["code"] == "devkum#M0.USD1"].iloc[0]
    # Both group label ("America") and leaf label ("USD 1") appear.
    assert "USD 1" in usd["dimension_path"]
    assert "America" in usd["dimension_path"]
    # Multi-dim cubes also include the other axis label.
    assert "Monthly average" in usd["dimension_path"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_snb_emits_complete_column_set() -> None:
    """Catalog completeness check — every documented column is populated
    (mirrors Treasury's column-shape test)."""
    _mock_all_known_cubes(
        live={
            "rendoblim": {
                "dimensions": _RENDOBLIM_DIMS,
                "csv": '"Date";"D0";"Value"\n"2024-01";"10J";"1.5"\n',
            },
        }
    )
    df = (await enumerate_snb(SnbEnumerateParams())).data
    expected = {
        "code",
        "title",
        "description",
        "source",
        "cube_id",
        "series_key",
        "dimension_path",
        "cube_title",
        "category",
        "frequency",
    }
    assert expected <= set(df.columns)


# ---------------------------------------------------------------------------
# Registry shape — sourced from the SNB nav tree, not hand-curation
# ---------------------------------------------------------------------------


def test_known_cubes_registry_covers_full_snb_portal() -> None:
    """Registry was harvested from the SNB nav tree; we assert a floor on
    coverage so a future regression that silently shrinks it (e.g. by
    losing a topic during re-discovery) trips a test rather than ships a
    half-empty catalog. 200 is a safe floor — audit count was 237 — and
    well above the original 17-cube hand-curated list (of which only 4
    were live)."""
    assert len(_snb_module._KNOWN_CUBES) >= 200, (
        f"registry shrank to {len(_snb_module._KNOWN_CUBES)} cubes — "
        "re-run the discovery script to refresh"
    )


def test_known_cubes_registry_uses_clean_cube_ids() -> None:
    """Every cube id must be a plain alphanumeric token — the SDMX-style
    warehouse cubes (``BSTA@SNB.AUR_U.ODF`` etc.) are *not* fetchable via
    ``/api/cube/{id}`` and would 500 if accidentally seeded into this
    list."""
    for cube_id, _ in _snb_module._KNOWN_CUBES:
        assert "@" not in cube_id and "." not in cube_id, (
            f"warehouse-style cube id leaked into registry: {cube_id!r}"
        )
        assert cube_id == cube_id.strip().lower(), (
            f"cube id should be lowercase trimmed: {cube_id!r}"
        )


def test_known_cubes_registry_has_no_duplicates() -> None:
    ids = [cid for cid, _ in _snb_module._KNOWN_CUBES]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# _series_from_dimensions — series-cap collapses mega-cubes to one row
# ---------------------------------------------------------------------------


def test_series_from_dimensions_collapses_oversized_cubes_to_cube_row() -> None:
    """Cubes with > _MAX_SERIES_PER_CUBE leaves emit one cube-level row
    (not the cartesian product). Mega-cubes like ``frsekfutsek`` (5,040
    crossings of currency × counterpart × maturity) would otherwise drown
    out semantic recall in the embedder; the cap keeps catalog size
    bounded while preserving the cube as a discoverable target."""
    # Synthesize a 3 × cap-overflow cube: dim with > _MAX_SERIES_PER_CUBE leaves.
    cap = _snb_module._MAX_SERIES_PER_CUBE
    big = {
        "cubeId": "huge",
        "dimensions": [
            {
                "id": "D0",
                "name": "Big dim",
                "dimensionItems": [
                    {"id": f"X{i}", "name": f"item {i}"} for i in range(cap + 5)
                ],
            }
        ],
    }
    rows = _series_from_dimensions("huge", "Mega cube", big)
    # Single cube-level row, not cap+5 rows.
    assert len(rows) == 1
    assert rows[0]["code"] == "huge#"
    assert rows[0]["series_key"] == ""
    assert rows[0]["cube_id"] == "huge"


def test_series_from_dimensions_preserves_series_for_in_cap_cubes() -> None:
    """Sanity: the cap doesn't flatten everything — modest cubes still
    yield one row per series exactly as before the cap was added."""
    rows = _series_from_dimensions(
        "rendoblim", "Yields", _RENDOBLIM_DIMS,
    )
    # 3 maturities, well under the cap.
    assert len(rows) == 3
    assert {r["series_key"] for r in rows} == {"1J", "10J", "30J"}
