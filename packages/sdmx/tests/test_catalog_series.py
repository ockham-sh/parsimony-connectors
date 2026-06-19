"""Tests for parquet-backed SDMX series catalogs."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.catalog import BM25Index, Catalog
from parsimony.errors import InvalidParameterError

from parsimony_sdmx.catalog_manifest import BuildRoot
from parsimony_sdmx.catalog_series import (
    CATALOG_KIND,
    build_flow_catalog,
    is_series_catalog,
)
from parsimony_sdmx.connectors import series_search
from parsimony_sdmx.connectors._agencies import AgencyId
from parsimony_sdmx.connectors.series_search import _clear_series_catalog_lru, sdmx_series_search
from parsimony_sdmx.core.models import (
    CodelistCode,
    CodelistRecord,
    DimensionStructure,
    StructureRecord,
)
from parsimony_sdmx.series_facets import facets_from_table
from parsimony_sdmx.series_fields import SERIES_PARQUET, dim_code_field, dim_label_field
from parsimony_sdmx.series_query import plan_series_search


def _load_build_all_catalogs() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "build_all_catalogs.py"
    spec = importlib.util.spec_from_file_location("sdmx_build_all_catalogs_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tiny_structure() -> StructureRecord:
    return StructureRecord(
        dataset_id="TEST",
        agency_id="ECB",
        title="Test flow",
        dsd_order=("FREQ", "REF_AREA"),
        dimensions=(
            DimensionStructure(
                dimension_id="FREQ",
                codelist_id="CL_FREQ",
                name="Frequency",
                code_count=2,
                sample=(CodelistCode(code="M", label="Monthly"),),
            ),
            DimensionStructure(
                dimension_id="REF_AREA",
                codelist_id="CL_GEO",
                name="Reference area",
                code_count=2,
                sample=(CodelistCode(code="DE", label="Germany"),),
            ),
        ),
        codelists=(
            CodelistRecord(
                codelist_id="CL_FREQ",
                codes=(
                    CodelistCode(code="M", label="Monthly"),
                    CodelistCode(code="A", label="Annual"),
                ),
            ),
            CodelistRecord(
                codelist_id="CL_GEO",
                codes=(
                    CodelistCode(code="DE", label="Germany"),
                    CodelistCode(code="FR", label="France"),
                ),
            ),
        ),
    )


def _sample_table() -> pa.Table:
    rows = [
        {
            "key": "M.DE",
            "title": "Monthly, Germany",
            "FREQ_code": "M",
            "FREQ_label": "Monthly",
            "REF_AREA_code": "DE",
            "REF_AREA_label": "Germany",
        },
        {
            "key": "A.DE",
            "title": "Annual, Germany",
            "FREQ_code": "A",
            "FREQ_label": "Annual",
            "REF_AREA_code": "DE",
            "REF_AREA_label": "Germany",
        },
        {
            "key": "M.FR",
            "title": "Monthly, France",
            "FREQ_code": "M",
            "FREQ_label": "Monthly",
            "REF_AREA_code": "FR",
            "REF_AREA_label": "France",
        },
    ]
    return pa.Table.from_pylist(rows)


def test_facets_returns_unpinned_dimension_counts() -> None:
    table = _sample_table().filter(pa.compute.equal(_sample_table()["REF_AREA_code"], "DE"))  # type: ignore[attr-defined]
    facets = facets_from_table(table, ("FREQ", "REF_AREA"), pinned_dims={"REF_AREA"})
    assert "FREQ" in facets
    assert ("M", "Monthly", 1) in facets["FREQ"]
    assert "REF_AREA" not in facets


def test_build_and_search_tiny_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    namespace = "sdmx_series_ecb_test"
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(_sample_table(), parquet)

    monkeypatch.setattr(
        "parsimony_sdmx.catalog_series._dim_label_index",
        lambda embedder: BM25Index(),
    )

    catalogs_dir = tmp_path / "catalogs"
    result = build_flow_catalog(
        series_parquet=parquet,
        namespace=namespace,
        agency=AgencyId.ECB,
        flow_id="TEST",
        structure=_tiny_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    assert result.catalog_kind == CATALOG_KIND
    catalog_dir = catalogs_dir / namespace
    assert is_series_catalog(catalog_dir)
    catalog = Catalog.load(f"file://{catalog_dir.resolve()}")
    plan = plan_series_search(
        "REF_AREA_label:germany && FREQ_code:M",
        catalog=catalog,
        dsd_order=("FREQ", "REF_AREA"),
        top_k_per_dim=5,
    )
    matches = catalog.search(
        plan.query,
        limit=10,
        field=plan.field,
        filter=plan.filter or None,
    )
    assert len(matches) == 1
    assert matches[0].code == "M.DE"


def test_broad_title_search_via_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Broad search on the title field must find series via the parquet backend."""
    namespace = "sdmx_series_ecb_test"
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(_sample_table(), parquet)

    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())

    catalogs_dir = tmp_path / "catalogs"
    build_flow_catalog(
        series_parquet=parquet,
        namespace=namespace,
        agency=AgencyId.ECB,
        flow_id="TEST",
        structure=_tiny_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    catalog = Catalog.load(f"file://{(catalogs_dir / namespace).resolve()}")

    matches = catalog.search("Monthly", limit=10)
    assert {m.code for m in matches} >= {"M.DE", "M.FR"}, f"Expected M.DE and M.FR in {[m.code for m in matches]}"

    matches_de = catalog.search("Germany", limit=10)
    assert {m.code for m in matches_de} >= {"M.DE", "A.DE"}, f"Expected M.DE and A.DE in {[m.code for m in matches_de]}"


def test_search_values_linked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """search_values must return linked codes for label fields."""
    namespace = "sdmx_series_ecb_test"
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(_sample_table(), parquet)

    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())

    catalogs_dir = tmp_path / "catalogs"
    build_flow_catalog(
        series_parquet=parquet,
        namespace=namespace,
        agency=AgencyId.ECB,
        flow_id="TEST",
        structure=_tiny_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    catalog = Catalog.load(f"file://{(catalogs_dir / namespace).resolve()}")

    values = catalog.search_values("Germany", field="REF_AREA_label", limit=5)
    assert values, "Expected at least one value match"
    germany_match = next((v for v in values if v.value == "Germany"), None)
    assert germany_match is not None, f"'Germany' not in {[v.value for v in values]}"
    assert germany_match.linked_value == "DE", f"Expected linked_value='DE', got {germany_match.linked_value!r}"


def test_dim_field_helpers() -> None:
    assert dim_code_field("FREQ") == "FREQ_code"
    assert dim_label_field("FREQ") == "FREQ_label"


def _build_searchable_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build the tiny TEST flow and return the catalog root to point the connector at."""
    namespace = "sdmx_series_ecb_test"
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(_sample_table(), parquet)
    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())
    catalogs_dir = tmp_path / "catalogs"
    build_flow_catalog(
        series_parquet=parquet,
        namespace=namespace,
        agency=AgencyId.ECB,
        flow_id="TEST",
        structure=_tiny_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    _clear_series_catalog_lru()
    return catalogs_dir


def test_series_search_surfaces_title(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Result rows must carry the human-readable title, not just opaque keys."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="Monthly",
        catalog_root=str(catalogs_dir),
    ).data

    assert "title" in df.columns
    titles = dict(zip(df["key"], df["title"], strict=True))
    assert titles.get("M.DE") == "Monthly, Germany"
    assert all(titles.values()), f"every row must carry a title, got {titles}"


def test_series_search_rejects_bare_dimension_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A bare dimension id in filter_json must fail fast with a corrective hint."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(InvalidParameterError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            query="Monthly",
            filter_json='{"FREQ": ["M"]}',
            catalog_root=str(catalogs_dir),
        )
    assert "FREQ_code" in str(exc.value)


def test_series_search_rejects_scalar_filter_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Filter values must be explicit lists, not strings accidentally expanded into characters."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(InvalidParameterError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            query="Monthly",
            filter_json='{"FREQ_code": "M"}',
            catalog_root=str(catalogs_dir),
        )
    assert "must be a list" in str(exc.value)


def test_series_search_code_filter_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The corrected ``{dim}_code`` filter key narrows results as expected."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="Monthly",
        filter_json='{"FREQ_code": ["M"]}',
        catalog_root=str(catalogs_dir),
    ).data

    keys = set(df["key"])
    assert keys <= {"M.DE", "M.FR"}
    assert "A.DE" not in keys


def test_resolve_catalog_path_downloads_hf_subdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote series search needs a real local directory so pyarrow can read series.parquet."""
    snapshot = tmp_path / "snapshot"
    catalog_dir = snapshot / "sdmx_series_ecb_test"
    catalog_dir.mkdir(parents=True)

    def fake_snapshot_download(**kwargs: object) -> str:
        assert kwargs["repo_id"] == "parsimony-dev/sdmx"
        assert kwargs["allow_patterns"] == ["sdmx_series_ecb_test/*"]
        return str(snapshot)

    monkeypatch.setattr("huggingface_hub.snapshot_download", fake_snapshot_download)
    monkeypatch.setattr(series_search, "lazy_catalog_dir", lambda provider, namespace: str(tmp_path / "empty-cache"))

    resolved = series_search._resolve_catalog_path("sdmx_series_ecb_test", catalog_root="hf://parsimony-dev/sdmx")
    assert resolved == catalog_dir


def test_fetch_done_uses_series_parquet_filename(tmp_path: Path) -> None:
    layout = BuildRoot.create(tmp_path)
    namespace = "sdmx_series_estat_demo"
    staging = layout.staging / "series" / namespace
    staging.mkdir(parents=True)
    (staging / "fetch_meta.json").write_text("{}", encoding="utf-8")
    (staging / SERIES_PARQUET).write_text("", encoding="utf-8")

    assert _load_build_all_catalogs()._fetch_done(layout, namespace)  # type: ignore[attr-defined]
