"""Tests for parquet-backed SDMX series catalogs."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from parsimony.catalog import BM25Index, Catalog, CatalogIndex
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
        fields=plan.field,
        filter=plan.filter or None,
    )
    assert len(matches) == 1
    assert matches[0].code == "M.DE"


def test_broad_query_finds_series_without_a_title_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A bare query still reaches every series — via dimension labels, not a title index.

    The composed title used to be indexed as one pseudo-member entity per distinct
    title (near 1:1 with rows). It carried nothing the ``{dim}_label`` indexes did
    not already hold, since it is those labels concatenated, so it was dropped.
    The recall it appeared to provide has to survive that, and it does: the query
    words ARE dimension labels.
    """
    namespace = "sdmx_series_ecb_test"
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(_sample_table(), parquet)

    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())

    catalogs_dir = tmp_path / "catalogs"
    build_flow_catalog(
        series_parquet=parquet,
        namespace=namespace,
        structure=_tiny_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    catalog = Catalog.load(f"file://{(catalogs_dir / namespace).resolve()}")

    # No title index is built, so no `__title__N` pseudo-member can exist in one.
    # (`catalog.entities` is empty by design here — attach_parquet_rows() discards
    # the member list into the indexes — so assert on the indexes themselves.)
    assert "title" not in catalog.indexes
    assert set(catalog.indexes) == {"FREQ_code", "FREQ_label", "REF_AREA_code", "REF_AREA_label"}

    surface = ["FREQ_label", "REF_AREA_label"]
    matches = catalog.search("Monthly", limit=10, fields=surface)
    assert {m.code for m in matches} >= {"M.DE", "M.FR"}, f"Expected M.DE and M.FR in {[m.code for m in matches]}"

    matches_de = catalog.search("Germany", limit=10, fields=surface)
    assert {m.code for m in matches_de} >= {"M.DE", "A.DE"}, f"Expected M.DE and A.DE in {[m.code for m in matches_de]}"

    # Every hit is a real series key served off the parquet, never a pseudo-member,
    # and each still carries its display title.
    for match in catalog.search("Germany", limit=10, fields=surface):
        assert not match.code.startswith("__title__")
        assert match.title


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
    ).raw

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
    ).raw

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
    ).raw
    listed = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="Monthly",
        filter_json='{"FREQ_code": ["M"]}',
        catalog_root=str(catalogs_dir),
    ).raw

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
            fields="FREQ_label",
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
    ).raw

    keys = set(df["key"])
    assert keys <= {"M.DE", "M.FR"}
    assert "A.DE" not in keys


def test_series_search_query_ranks_within_filter_slice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """query= alongside filter_json= (no fields=) ranks inside the filtered slice.

    Regression: the query used to be dropped when filter_json= was supplied
    without fields=, so the call returned the whole slice unranked. It must
    instead rank the slice by the query — here the filter admits both monthly
    series (M.DE, M.FR) and the query "France" keeps only the French one.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="France",
        filter_json='{"FREQ_code": ["M"]}',
        catalog_root=str(catalogs_dir),
    ).raw

    assert set(df["key"]) == {"M.FR"}


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
    ).raw

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


def _bop_shaped_structure() -> StructureRecord:
    return StructureRecord(
        dataset_id="BOPX",
        agency_id="ECB",
        title="BOP-shaped flow",
        dsd_order=("FREQ", "ITEM"),
        dimensions=(
            DimensionStructure(
                dimension_id="FREQ",
                codelist_id="CL_FREQ",
                name="Frequency",
                code_count=1,
                sample=(CodelistCode(code="Q", label="Quarterly"),),
            ),
            DimensionStructure(
                dimension_id="ITEM",
                codelist_id="CL_ITEM",
                name="BOP item",
                code_count=3,
                sample=(CodelistCode(code="993", label="Current account"),),
            ),
        ),
        codelists=(
            CodelistRecord(
                codelist_id="CL_FREQ",
                codes=(CodelistCode(code="Q", label="Quarterly"),),
            ),
            CodelistRecord(
                codelist_id="CL_ITEM",
                codes=(
                    CodelistCode(code="993", label="Current account"),
                    CodelistCode(code="379", label="Current account - Goods"),
                    CodelistCode(code="391", label="Current account - Services"),
                ),
            ),
        ),
    )


def test_series_search_bare_query_spans_dimension_labels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A bare query that exactly names a dimension value must surface that slice first.

    The ECB/BOP field repro: children repeat the parent phrase in their composed
    titles, so title-only scoring buries the aggregate (rank 13 behind twelve
    sub-items live). The aggregate's own ITEM label IS the query — the bare-query
    surface (title + dimension labels) pins it at rank 1.
    """
    rows = [
        {
            # The aggregate's title does NOT contain the query terms.
            "key": "Q.993",
            "title": "Quarterly, Total economy aggregate",
            "FREQ_code": "Q",
            "FREQ_label": "Quarterly",
            "ITEM_code": "993",
            "ITEM_label": "Current account",
        },
        {
            "key": "Q.379",
            "title": "Quarterly, Current account - Goods",
            "FREQ_code": "Q",
            "FREQ_label": "Quarterly",
            "ITEM_code": "379",
            "ITEM_label": "Current account - Goods",
        },
        {
            "key": "Q.391",
            "title": "Quarterly, Current account - Services",
            "FREQ_code": "Q",
            "FREQ_label": "Quarterly",
            "ITEM_code": "391",
            "ITEM_label": "Current account - Services",
        },
    ]
    parquet = tmp_path / SERIES_PARQUET
    pq.write_table(pa.Table.from_pylist(rows), parquet)
    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())
    catalogs_dir = tmp_path / "catalogs"
    build_flow_catalog(
        series_parquet=parquet,
        namespace="sdmx_series_ecb_bopx",
        structure=_bop_shaped_structure(),
        catalogs_dir=catalogs_dir,
        staging_dir=tmp_path / "partial",
    )
    _clear_series_catalog_lru()

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="BOPX",
        query="current account",
        catalog_root=str(catalogs_dir),
    ).raw

    keys = list(df["key"])
    # Title-only scoring cannot rank Q.993 at all; the label surface pins it first.
    assert keys[0] == "Q.993"
    # Title candidates still participate in the fused ranking below the pin.
    assert {"Q.379", "Q.391"} <= set(keys[1:])
    # The ranking evidence is explicit: the aggregate's ITEM label is the query
    # (coverage 1.0); the children earn no coverage and rank by fuzzy score.
    assert df["coverage"].iloc[0] == 1.0
    assert all(df["coverage"].iloc[1:] < 1.0)
    assert all(df["score"] < 1_000.0)


# ---------------------------------------------------------------------------
# title is a display column, never a search surface (#66)
# ---------------------------------------------------------------------------


def _title_error_message(exc: pytest.ExceptionInfo[InvalidParameterError]) -> str:
    return str(exc.value)


@pytest.mark.parametrize(
    ("kwargs", "route"),
    [
        ({"query": "Monthly", "fields": "title"}, "fields="),
        ({"query": "title: Monthly"}, "single title: clause"),
        ({"query": "title: Monthly && FREQ_code: M"}, "title: clause among others"),
    ],
)
def test_every_route_to_searching_title_gets_the_same_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, str], route: str
) -> None:
    """Asking to search ``title`` fails with an explanation, not a silent miss.

    Three routes reach it — ``fields=``, a lone ``title:`` clause, and a ``title:``
    clause beside others — and they are the same mistake, so they get the same
    answer. Without the interception the clause routes surface the kernel's generic
    "field is not indexed", which is true but says neither that title is
    deliberately unindexed nor what to do instead.
    """
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    with pytest.raises(InvalidParameterError) as exc:
        sdmx_series_search(agency="ECB", dataset_id="TEST", catalog_root=str(catalogs_dir), **kwargs)

    message = _title_error_message(exc)
    assert "display column" in message, f"{route}: {message}"
    assert "{dim}_label" in message, f"{route}: {message}"


def test_title_is_still_filterable_without_an_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``filter_json`` on title keeps working — it is a parquet read, not a scored search."""
    catalogs_dir = _build_searchable_catalog(tmp_path, monkeypatch)

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        filter_json='{"title": ["Monthly, Germany"]}',
        catalog_root=str(catalogs_dir),
    ).raw

    assert set(df["key"]) == {"M.DE"}


def _build_legacy_titled_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Build the catalog the OLD way: a ``title`` BM25 index + ``__title__N`` members.

    This is the shape of every catalog published before the index was dropped, and
    they stay on the hub until republished — so the new code has to keep serving them.
    """
    from parsimony.catalog.contracts import CatalogBackendConfig
    from parsimony.entity import Entity

    import parsimony_sdmx.catalog_series as cs

    namespace = "sdmx_series_ecb_test"
    parquet = tmp_path / SERIES_PARQUET
    table = _sample_table()
    pq.write_table(table, parquet)
    monkeypatch.setattr("parsimony_sdmx.catalog_series._dim_label_index", lambda embedder: BM25Index())

    dsd_order = ["FREQ", "REF_AREA"]
    distinct = cs.collect_distinct_from_columnar(parquet, tuple(dsd_order))
    entities = cs._index_entities_for_distinct(namespace, dsd_order, distinct)
    titles = sorted({str(t) for t in table.column("title").to_pylist()})
    entities += [
        Entity(namespace=namespace, code=f"__title__{i}", title=t, metadata={"title": t})
        for i, t in enumerate(titles)
    ]

    indexes: dict[str, CatalogIndex] = {"title": BM25Index()}
    for dim in dsd_order:
        indexes[dim_code_field(dim)] = BM25Index()
        indexes[dim_label_field(dim)] = BM25Index()

    catalog = Catalog(
        namespace,
        indexes=indexes,
        field_links={dim_label_field(d): dim_code_field(d) for d in dsd_order},
    )
    catalog.set_entities(entities)
    catalog.build()
    catalog.attach_parquet_rows(
        parquet,
        config=CatalogBackendConfig(
            kind="parquet",
            rows_path=SERIES_PARQUET,
            namespace=namespace,
            code_column="key",
            title_column="title",
        ),
    )
    catalogs_dir = tmp_path / "catalogs"
    (catalogs_dir / namespace).mkdir(parents=True)
    catalog.save(str(catalogs_dir / namespace))
    _clear_series_catalog_lru()
    return catalogs_dir


def test_already_published_catalogs_still_serve(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A snapshot built the old way must keep working until it is republished.

    Dropping the index is a build-side change; ~7.9k catalogs on the hub still carry
    a ``title`` index and one ``__title__N`` pseudo-member per distinct title. Those
    members must never surface as results — they are not series.
    """
    catalogs_dir = _build_legacy_titled_catalog(tmp_path, monkeypatch)

    df = sdmx_series_search(
        agency="ECB",
        dataset_id="TEST",
        query="Monthly Germany",
        catalog_root=str(catalogs_dir),
    ).raw

    assert df.iloc[0]["key"] == "M.DE"
    assert not any(str(k).startswith("__title__") for k in df["key"]), df["key"].tolist()
    assert df["title"].map(bool).all()
