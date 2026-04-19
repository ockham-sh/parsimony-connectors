from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

from parsimony_sdmx.cli.worker import run_dataset
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.core.outcomes import FailureKind, OutcomeStatus


def _series_stream() -> Iterator[SeriesRecord]:
    yield SeriesRecord(id="A.U2", dataset_id="YC", title="t1")
    yield SeriesRecord(id="A.US", dataset_id="YC", title="t2")


class TestRunDataset:
    def test_happy_path(self, tmp_path: Path) -> None:
        mock_provider = MagicMock()
        mock_provider.list_series.return_value = _series_stream()

        with patch(
            "parsimony_sdmx.providers.registry.get_provider", return_value=mock_provider
        ):
            outcome = run_dataset("ECB", "YC", str(tmp_path))

        assert outcome.status == OutcomeStatus.OK
        assert outcome.rows == 2
        assert outcome.bytes > 0
        assert outcome.parquet_path is not None
        assert Path(outcome.parquet_path).exists()
        assert outcome.duration_s >= 0
        assert outcome.started_at is not None
        assert outcome.finished_at is not None

    def test_empty_series_stream_yields_empty_status(self, tmp_path: Path) -> None:
        mock_provider = MagicMock()
        mock_provider.list_series.return_value = iter([])

        with patch(
            "parsimony_sdmx.providers.registry.get_provider", return_value=mock_provider
        ):
            outcome = run_dataset("ECB", "YC", str(tmp_path))

        assert outcome.status == OutcomeStatus.EMPTY
        assert outcome.rows == 0

    def test_bare_value_error_classified_as_unknown(self, tmp_path: Path) -> None:
        # Narrowed: a bare ValueError from provider code is a programmer bug,
        # not a parse error. It should surface as UNKNOWN so it stands out.
        mock_provider = MagicMock()
        mock_provider.list_series.side_effect = ValueError("boom")

        with patch(
            "parsimony_sdmx.providers.registry.get_provider", return_value=mock_provider
        ):
            outcome = run_dataset("ECB", "YC", str(tmp_path))

        assert outcome.status == OutcomeStatus.FAILED
        assert outcome.kind == FailureKind.UNKNOWN
        assert "boom" in (outcome.error_message or "")

    def test_unknown_agency_classified_as_unknown(self, tmp_path: Path) -> None:
        # KeyError from get_provider("NOPE") now falls through to UNKNOWN.
        outcome = run_dataset("NOPE", "YC", str(tmp_path))
        assert outcome.status == OutcomeStatus.FAILED
        assert outcome.kind == FailureKind.UNKNOWN

    def test_title_build_error_classified_as_parse_error(self, tmp_path: Path) -> None:
        from parsimony_sdmx.core.errors import TitleBuildError

        mock_provider = MagicMock()
        mock_provider.list_series.side_effect = TitleBuildError("bad codelist")

        with patch(
            "parsimony_sdmx.providers.registry.get_provider", return_value=mock_provider
        ):
            outcome = run_dataset("ECB", "YC", str(tmp_path))

        assert outcome.status == OutcomeStatus.FAILED
        assert outcome.kind == FailureKind.PARSE_ERROR
