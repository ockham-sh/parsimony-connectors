from collections.abc import Iterator

import pytest

from parsimony_sdmx.core.errors import TitleBuildError
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.core.projection import project_series


class TestProjectSeries:
    def test_basic_yield(self) -> None:
        labels = {
            "FREQ": {"A": "Annual", "M": "Monthly"},
            "REF_AREA": {"U2": "Euro area"},
        }
        series = [
            {"FREQ": "A", "REF_AREA": "U2"},
            {"FREQ": "M", "REF_AREA": "U2"},
        ]
        out = list(
            project_series(
                dataset_id="YC",
                series_dim_values=series,
                dsd_order=("FREQ", "REF_AREA"),
                labels=labels,
            )
        )
        assert out == [
            SeriesRecord(
                id="A.U2",
                dataset_id="YC",
                title="A: Annual - U2: Euro area",
                fragments=("Annual", "Euro area"),
            ),
            SeriesRecord(
                id="M.U2",
                dataset_id="YC",
                title="M: Monthly - U2: Euro area",
                fragments=("Monthly", "Euro area"),
            ),
        ]

    def test_generator_streams_without_materialising(self) -> None:
        produced: list[int] = []

        def gen() -> Iterator[dict[str, str]]:
            for i in range(5):
                produced.append(i)
                yield {"FREQ": f"F{i}"}

        it = project_series(
            dataset_id="YC",
            series_dim_values=gen(),
            dsd_order=("FREQ",),
            labels={},
        )
        # First pull advances generator by exactly one
        first = next(it)
        assert produced == [0]
        assert first.id == "F0"
        # Drain rest
        rest = list(it)
        assert len(rest) == 4
        assert produced == [0, 1, 2, 3, 4]

    def test_series_id_is_dotted_dsd_order(self) -> None:
        series = [{"B": "y", "A": "x", "C": "z"}]
        out = list(
            project_series(
                dataset_id="D",
                series_dim_values=series,
                dsd_order=("A", "B", "C"),
                labels={},
            )
        )
        assert out[0].id == "x.y.z"

    def test_raw_code_fallback_propagates_to_title(self) -> None:
        out = list(
            project_series(
                dataset_id="D",
                series_dim_values=[{"A": "x"}],
                dsd_order=("A",),
                labels={},  # no labels → raw-code fallback
            )
        )
        assert out[0].title == "x"

    def test_fragments_fall_back_to_code_when_label_missing(self) -> None:
        """Dims with no codelist still contribute a fragment (the raw code)."""
        out = list(
            project_series(
                dataset_id="D",
                series_dim_values=[{"A": "x", "B": "y"}],
                dsd_order=("A", "B"),
                labels={"A": {"x": "X-label"}},  # B has no labels
            )
        )
        assert out[0].fragments == ("X-label", "y")

    def test_fragments_dedup_candidates_at_flow_scale(self) -> None:
        """Common dims ("Monthly", "Euro area") repeat verbatim across rows.

        This is exactly the dedup window that FragmentEmbeddingCache
        exploits: two series share the same "Monthly" + "Euro area"
        fragment strings even when their other dims differ. Asserting
        string equality here is the contract the cache relies on.
        """
        labels = {
            "FREQ": {"M": "Monthly"},
            "REF_AREA": {"U2": "Euro area"},
            "INDICATOR": {"X": "Ind X", "Y": "Ind Y"},
        }
        series = [
            {"FREQ": "M", "REF_AREA": "U2", "INDICATOR": "X"},
            {"FREQ": "M", "REF_AREA": "U2", "INDICATOR": "Y"},
        ]
        out = list(
            project_series(
                dataset_id="D",
                series_dim_values=series,
                dsd_order=("FREQ", "REF_AREA", "INDICATOR"),
                labels=labels,
            )
        )
        assert out[0].fragments[0] == out[1].fragments[0] == "Monthly"
        assert out[0].fragments[1] == out[1].fragments[1] == "Euro area"
        assert out[0].fragments[2] != out[1].fragments[2]

    def test_augment_hook_called_with_series_id(self) -> None:
        seen: list[tuple[str, str]] = []

        def augment(base: str, sid: str) -> str:
            seen.append((base, sid))
            return f"{base} | AUG"

        out = list(
            project_series(
                dataset_id="D",
                series_dim_values=[{"A": "x"}, {"A": "y"}],
                dsd_order=("A",),
                labels={"A": {"x": "X", "y": "Y"}},
                augment=augment,
            )
        )
        assert seen == [("x: X", "x"), ("y: Y", "y")]
        assert out[0].title == "x: X | AUG"
        assert out[1].title == "y: Y | AUG"

    def test_missing_dim_value_raises_title_build_error(self) -> None:
        with pytest.raises(TitleBuildError, match="missing value"):
            list(
                project_series(
                    dataset_id="D",
                    series_dim_values=[{"A": "x"}],  # missing B
                    dsd_order=("A", "B"),
                    labels={},
                )
            )

    def test_empty_dsd_order_raises(self) -> None:
        with pytest.raises(TitleBuildError, match="dsd_order is empty"):
            list(
                project_series(
                    dataset_id="D",
                    series_dim_values=[{}],
                    dsd_order=(),
                    labels={},
                )
            )

    def test_empty_series_stream_yields_nothing(self) -> None:
        out = list(
            project_series(
                dataset_id="D",
                series_dim_values=[],
                dsd_order=("A",),
                labels={},
            )
        )
        assert out == []

    def test_dataset_id_stamped_on_every_record(self) -> None:
        series = [{"A": "x"}, {"A": "y"}, {"A": "z"}]
        out = list(
            project_series(
                dataset_id="WDI",
                series_dim_values=series,
                dsd_order=("A",),
                labels={},
            )
        )
        assert all(r.dataset_id == "WDI" for r in out)
