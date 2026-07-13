"""Tests for parquet-backed SDMX series catalogs."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.catalog import BM25Index, Catalog
from parsimony.errors import ConnectorError, EmptyDataError, InvalidParameterError

from parsimony_sdmx.catalog_manifest import BuildRoot
from parsimony_sdmx.catalog_series import (
    CATALOG_KIND,
    build_flow_catalog,
    is_series_catalog,
)
from parsimony_sdmx.connectors import dimension_search, series_search
from parsimony_sdmx.connectors.dimension_search import sdmx_dimension_search
from parsimony_sdmx.connectors.series_search import _clear_series_catalog_lru, sdmx_series_search
from parsimony_sdmx.core.agencies import AgencyId
from parsimony_sdmx.core.models import (
    CodelistCode,
    CodelistRecord,
    DimensionStructure,
    StructureRecord,
)
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


def test_series_search_filter_only_allows_enumeration_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A filter-only read of the cached catalog may exceed the ranked cap of 500.

    The field report hit this: a 574-series dimension slice was silently truncated at
    500. A filter_json read is an enumeration into a variable, not a ranked shortlist,
    so it must accept a much larger limit.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        filter_json='{"REF_AREA_code": ["DE"]}',
        limit=5000,
        catalog_root=str(catalogs_dir),
    ).data

    assert set(df["key"]) == {"M.DE", "A.DE"}


def test_series_search_ranked_query_rejects_enumeration_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A ranked (free-text) query stays a shortlist; a huge limit is refused with a hint."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(InvalidParameterError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            query="Monthly",
            limit=5000,
            catalog_root=str(catalogs_dir),
        )
    assert "filter_json" in str(exc.value)


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


def test_series_search_coerces_scalar_filter_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A bare scalar filter value is accepted as a single code, equivalent to a 1-element list.

    A str is iterable, so the coercion must wrap it (``"M"`` -> ``["M"]``), never iterate it
    into characters — the scalar form must return exactly what the list form does.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    scalar = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="Monthly",
        filter_json='{"FREQ_code": "M"}',
        catalog_root=str(catalogs_dir),
    ).data
    listed = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="Monthly",
        filter_json='{"FREQ_code": ["M"]}',
        catalog_root=str(catalogs_dir),
    ).data

    assert set(scalar["key"]) == set(listed["key"])
    assert set(scalar["key"]) <= {"M.DE", "M.FR"}
    assert "A.DE" not in set(scalar["key"])


def test_series_search_rejects_unpopulated_filter_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A filter value the flow never populates must fail fast, not silently drop (issue #48).

    ``isin`` semantics would return DE rows and omit EL with no signal; instead the
    call raises naming the missing value, the matched/requested counts, and the
    ``sdmx_dimension_search`` recovery path.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(InvalidParameterError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            filter_json='{"REF_AREA_code": ["DE", "EL"]}',
            catalog_root=str(catalogs_dir),
        )
    msg = str(exc.value)
    assert "'EL'" in msg
    assert "1 of 2" in msg
    assert "sdmx_dimension_search" in msg
    assert "dimension='REF_AREA'" in msg


def test_series_search_rejects_unpopulated_label_filter_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Value validation covers ``_label`` filter columns the same as ``_code`` ones."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(InvalidParameterError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            filter_json='{"REF_AREA_label": ["Germany", "Atlantis"]}',
            catalog_root=str(catalogs_dir),
        )
    msg = str(exc.value)
    assert "'Atlantis'" in msg
    assert "dimension='REF_AREA'" in msg


def test_series_search_empty_combination_reports_standalone_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unpopulated AND-combination reports each column's standalone match count (issue #48).

    Both A (via A.DE) and FR (via M.FR) exist individually, but no A.FR series does —
    the EmptyDataError must say so instead of echoing the filter back verbatim.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(EmptyDataError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            filter_json='{"FREQ_code": ["A"], "REF_AREA_code": ["FR"]}',
            catalog_root=str(catalogs_dir),
        )
    msg = str(exc.value)
    assert "FREQ_code=['A'] -> 1 series alone" in msg
    assert "REF_AREA_code=['FR'] -> 1 series alone" in msg
    # Leave-one-out names the conflicting pair: dropping either column unblocks the other.
    assert "FREQ_code (-> 1 series)" in msg
    assert "REF_AREA_code (-> 1 series)" in msg
    assert "conflict lies among these" in msg


def test_series_search_query_elimination_blames_query(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the filter matches series but the field-scoped query eliminates them all,
    the EmptyDataError blames the query, not the filter."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(EmptyDataError) as exc:
        sdmx_series_search(
            agency="ECB",
            dataset_id="TEST",
            query="Quarterly",
            field="title",
            filter_json='{"FREQ_code": ["M"]}',
            catalog_root=str(catalogs_dir),
        )
    msg = str(exc.value)
    assert "the filter alone matches 2 series" in msg
    assert "Quarterly" in msg


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


def test_series_search_strips_flow_prefixed_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keys from an old catalog carrying the flow prefix ("TEST.M.DE") emit bare.

    New catalogs strip the prefix at build time, but published ones can predate that —
    the emitted `key` must always equal sdmx_fetch's bare `series_key` so the two
    connectors' outputs join without string surgery.
    """
    namespace = "sdmx_series_ecb_test"
    rows = _sample_table().to_pylist()
    for row in rows:
        row["key"] = f"TEST.{row['key']}"
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(pa.Table.from_pylist(rows), parquet)
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

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        filter_json='{"REF_AREA_code": ["DE"]}',
        catalog_root=str(catalogs_dir),
    ).data

    assert set(df["key"]) == {"M.DE", "A.DE"}
    # The title lookup happens on the raw (still-prefixed) keys and must survive the strip.
    assert all(t for t in df["title"]), f"titles lost in prefix strip: {df.to_dict('records')}"


def test_series_search_query_no_match_message_guides_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The free-text empty case gets recovery guidance, not just a bare 'no match'."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(EmptyDataError) as exc:
        sdmx_series_search(agency="ECB", dataset_id="TEST", query="zebra population", catalog_root=str(catalogs_dir))
    msg = str(exc.value)
    assert "3 series in the flow's catalog" in msg
    assert "sdmx_dimension_search" in msg


def test_dimension_search_corrupt_catalog_raises_connector_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A corrupt catalog (the framework's sha256 integrity ValueError) must surface as a typed
    ConnectorError from sdmx_dimension_search, not leak a raw ValueError — matching
    sdmx_series_search, which already wraps the same failure.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    def raise_integrity(namespace: str, catalog_path: str) -> object:
        raise ValueError("Catalog snapshot integrity check failed")

    monkeypatch.setattr(dimension_search, "_load_series_catalog", raise_integrity)

    with pytest.raises(ConnectorError, match="Invalid series catalog"):
        sdmx_dimension_search(
            agency="ECB", dataset_id="TEST", dimension="FREQ", query="Monthly", catalog_root=str(catalogs_dir)
        )


def test_resolve_catalog_path_downloads_hf_subdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote series search resolves to a real local dir via core's path-scoped fetch.

    It must delegate to ``download_hf_subpath`` (scoped listing) rather than re-issue
    ``snapshot_download(allow_patterns=...)``, which enumerates the whole 17k-file
    SDMX monorepo before filtering and hangs for minutes on a cold catalog.
    """
    catalog_dir = tmp_path / "snapshot" / "sdmx_series_ecb_test"
    catalog_dir.mkdir(parents=True)
    calls: dict[str, object] = {}

    def fake_resolve_catalog_dir(url: str, *, cache_dir: object = None) -> Path:
        calls["url"] = url
        return catalog_dir

    monkeypatch.setattr(series_search, "resolve_catalog_dir", fake_resolve_catalog_dir)
    monkeypatch.setattr(series_search, "lazy_catalog_dir", lambda provider, namespace: str(tmp_path / "empty-cache"))

    resolved = series_search._resolve_catalog_path(
        "sdmx_series_ecb_test", label="ECB/TEST", catalog_root="hf://parsimony-dev/sdmx"
    )
    assert resolved == catalog_dir
    # The connector hands the framework a plain URL; it holds no scheme logic itself.
    assert calls == {"url": "hf://parsimony-dev/sdmx/sdmx_series_ecb_test"}


def test_resolve_catalog_path_unsupported_scheme_raises_connector_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bad catalog URL scheme surfaces as ConnectorError, not a bare ValueError.

    Resolution is delegated to the framework, which raises ``ValueError`` for an
    unknown scheme; the connector re-wraps it so callers catching ``ConnectorError``
    still see the failure.
    """
    monkeypatch.setattr(series_search, "lazy_catalog_dir", lambda provider, namespace: str(tmp_path / "empty-cache"))

    with pytest.raises(ConnectorError):
        series_search._resolve_catalog_path(
            "sdmx_series_ecb_test", label="ECB/TEST", catalog_root="ftp://example.com/repo"
        )


@pytest.mark.parametrize("exc_factory", ["entry_not_found", "catalog_not_found"])
def test_resolve_catalog_path_unpublished_flow_gives_friendly_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exc_factory: str
) -> None:
    """A missing (hf 404) or empty remote sub-tree becomes the "not published" message.

    Regression: ``resolve_catalog_dir`` raises ``huggingface_hub`` ``EntryNotFoundError`` for a
    flow that was never built on the default ``hf://`` root — previously that raw 404 (with an
    internal request id + API URL) leaked straight to the caller instead of the documented
    guardrail. Both it and the framework's empty-bundle ``CatalogNotFoundError`` must surface as
    one friendly ``ConnectorError``.
    """
    from huggingface_hub.errors import EntryNotFoundError
    from parsimony.errors import CatalogNotFoundError

    exc: Exception = (
        EntryNotFoundError("404 Entry Not Found") if exc_factory == "entry_not_found" else CatalogNotFoundError("empty")
    )

    def raise_not_found(url: str, *, cache_dir: object = None) -> Path:
        raise exc

    monkeypatch.setattr(series_search, "resolve_catalog_dir", raise_not_found)
    monkeypatch.setattr(series_search, "lazy_catalog_dir", lambda provider, namespace: str(tmp_path / "empty-cache"))

    with pytest.raises(ConnectorError, match="not published"):
        series_search._resolve_catalog_path(
            "sdmx_series_ecb_test", label="ECB/TEST", catalog_root="hf://parsimony-dev/sdmx"
        )


def test_resolve_catalog_path_network_error_propagates_raw(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreachable Hub is NOT "not published" — its error must propagate untranslated.

    Only the missing/empty-catalog exceptions map to the friendly message; a transport failure
    is a distinct condition the caller must be able to see (and retry) as itself.
    """

    def raise_network(url: str, *, cache_dir: object = None) -> Path:
        raise ConnectionError("Hub unreachable")

    monkeypatch.setattr(series_search, "resolve_catalog_dir", raise_network)
    monkeypatch.setattr(series_search, "lazy_catalog_dir", lambda provider, namespace: str(tmp_path / "empty-cache"))

    with pytest.raises(ConnectionError):
        series_search._resolve_catalog_path(
            "sdmx_series_ecb_test", label="ECB/TEST", catalog_root="hf://parsimony-dev/sdmx"
        )


def test_fetch_done_uses_series_parquet_filename(tmp_path: Path) -> None:
    layout = BuildRoot.create(tmp_path)
    namespace = "sdmx_series_estat_demo"
    staging = layout.staging / "series" / namespace
    staging.mkdir(parents=True)
    (staging / "fetch_meta.json").write_text("{}", encoding="utf-8")
    (staging / SERIES_PARQUET).write_text("", encoding="utf-8")

    assert _load_build_all_catalogs()._fetch_done(layout, namespace)  # type: ignore[attr-defined]


class TestStripFlowPrefix:
    """A raw SDMX-CSV ``KEY`` column value is not always the bare key ``sdmx_fetch`` expects.

    Regression coverage for the ECB export prefixing ``KEY`` with the flow id
    (``"YC.B.U2...."`` instead of ``"B.U2...."``), which broke the documented
    ``sdmx_series_search`` → ``sdmx_fetch`` idiom.
    """

    def test_strips_matching_prefix(self) -> None:
        from parsimony_sdmx.catalog_series import _strip_flow_prefix

        assert _strip_flow_prefix("YC.B.U2.EUR", "YC") == "B.U2.EUR"

    def test_case_insensitive(self) -> None:
        from parsimony_sdmx.catalog_series import _strip_flow_prefix

        assert _strip_flow_prefix("yc.B.U2.EUR", "YC") == "B.U2.EUR"

    def test_leaves_bare_key_untouched(self) -> None:
        from parsimony_sdmx.catalog_series import _strip_flow_prefix

        assert _strip_flow_prefix("B.U2.EUR", "YC") == "B.U2.EUR"

    def test_does_not_touch_unrelated_leading_segment(self) -> None:
        from parsimony_sdmx.catalog_series import _strip_flow_prefix

        # A dimension code that happens to equal the dataset_id is not a flow prefix.
        assert _strip_flow_prefix("YC.YC.EUR", "YC") == "YC.EUR"


class TestSeriesRowDictKeyPrefix:
    """``_series_row_dict`` must emit the bare key even when the CSV's own ``KEY`` column is flow-prefixed."""

    def test_key_column_with_flow_prefix_is_stripped(self) -> None:
        from parsimony_sdmx.catalog_series import _series_row_dict

        row = ["YC.M.DE", "M", "DE"]
        record = _series_row_dict(
            row=row,
            col_indices=[1, 2],
            dim_ids=["FREQ", "REF_AREA"],
            dsd_order=("FREQ", "REF_AREA"),
            labels={},
            key_idx=0,
            dataset_id="YC",
        )
        assert record is not None
        assert record["key"] == "M.DE"

    def test_key_column_without_prefix_is_unaffected(self) -> None:
        from parsimony_sdmx.catalog_series import _series_row_dict

        row = ["M.DE", "M", "DE"]
        record = _series_row_dict(
            row=row,
            col_indices=[1, 2],
            dim_ids=["FREQ", "REF_AREA"],
            dsd_order=("FREQ", "REF_AREA"),
            labels={},
            key_idx=0,
            dataset_id="YC",
        )
        assert record is not None
        assert record["key"] == "M.DE"

    def test_fallback_key_from_dim_values_is_already_bare(self) -> None:
        """No ``KEY`` column at all: the dim-values-joined fallback never carried the bug."""
        from parsimony_sdmx.catalog_series import _series_row_dict

        row = ["M", "DE"]
        record = _series_row_dict(
            row=row,
            col_indices=[0, 1],
            dim_ids=["FREQ", "REF_AREA"],
            dsd_order=("FREQ", "REF_AREA"),
            labels={},
            key_idx=None,
            dataset_id="YC",
        )
        assert record is not None
        assert record["key"] == "M.DE"
