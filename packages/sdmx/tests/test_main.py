"""Tests for cli/main.py: dispatch, exit codes, --force warning, dry-run output.

The parent no longer imports provider modules — every listing call goes
through :func:`parsimony_sdmx.cli.listing.list_datasets`, which is what
tests patch.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from parsimony_sdmx.cli.listing import ListDatasetsError
from parsimony_sdmx.cli.main import _exit_code, main
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.core.outcomes import DatasetOutcome, FailureKind, OutcomeStatus


def _recs(*pairs: tuple[str, str]) -> list[DatasetRecord]:
    return [
        DatasetRecord(dataset_id=d, agency_id="ECB", title=t) for d, t in pairs
    ]


class TestExitCode:
    def _outcome(self, status: OutcomeStatus) -> DatasetOutcome:
        return DatasetOutcome(
            dataset_id="X", agency_id="ECB", status=status
        )

    def test_empty_is_ok(self) -> None:
        assert _exit_code([]) == 0

    def test_all_ok(self) -> None:
        assert _exit_code(
            [self._outcome(OutcomeStatus.OK), self._outcome(OutcomeStatus.OK)]
        ) == 0

    def test_all_empty_is_ok(self) -> None:
        assert _exit_code([self._outcome(OutcomeStatus.EMPTY)]) == 0

    def test_any_failed_is_1(self) -> None:
        assert _exit_code(
            [self._outcome(OutcomeStatus.OK), self._outcome(OutcomeStatus.FAILED)]
        ) == 1

    def test_all_failed_is_1(self) -> None:
        assert _exit_code(
            [self._outcome(OutcomeStatus.FAILED), self._outcome(OutcomeStatus.FAILED)]
        ) == 1


class TestListDatasets:
    def test_prints_tsv_and_exits_ok(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        recs = _recs(("YC", "Yield curve"), ("CPI", "Consumer prices"))
        with patch(
            "parsimony_sdmx.cli.main.list_datasets", return_value=recs
        ):
            rc = main(["-a", "ECB", "--list-datasets", "-o", str(tmp_path)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "YC\tYield curve" in out
        assert "CPI\tConsumer prices" in out

    def test_listing_failure_returns_non_zero(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        with patch(
            "parsimony_sdmx.cli.main.list_datasets",
            side_effect=ListDatasetsError("http_error", "upstream 503", ""),
        ):
            rc = main(["-a", "ECB", "--list-datasets", "-o", str(tmp_path)])
        assert rc == 1
        msgs = "\n".join(r.getMessage() for r in caplog.records)
        assert "ECB" in msgs
        assert "http_error" in msgs


class TestCatalog:
    def test_writes_datasets_parquet_and_prints_count(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        recs = _recs(("YC", "Yield curve"))
        with patch(
            "parsimony_sdmx.cli.main.list_datasets", return_value=recs
        ):
            rc = main(["-a", "ECB", "--catalog", "-o", str(tmp_path)])
        assert rc == 0
        assert (tmp_path / "ECB" / "datasets.parquet").exists()
        out = capsys.readouterr().out
        assert "Wrote 1 dataset(s)" in out

    def test_catalog_failure_returns_non_zero(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        with patch(
            "parsimony_sdmx.cli.main.list_datasets",
            side_effect=ListDatasetsError("http_error", "upstream 503", ""),
        ):
            rc = main(["-a", "ECB", "--catalog", "-o", str(tmp_path)])
        assert rc == 1
        msgs = "\n".join(r.getMessage() for r in caplog.records)
        assert "ECB" in msgs
        assert "http_error" in msgs


class TestDryRun:
    def test_all_dry_run_prints_paths_and_counts(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        recs = _recs(("YC", "t1"), ("CPI", "t2"))
        with patch(
            "parsimony_sdmx.cli.main.list_datasets", return_value=recs
        ):
            rc = main(
                ["-a", "ECB", "--all", "--dry-run", "-o", str(tmp_path)]
            )
        assert rc == 0
        out = capsys.readouterr().out
        assert "Would process 2 dataset" in out
        assert "2 new" in out
        assert "datasets.parquet" in out
        assert "YC.parquet" in out
        assert "CPI.parquet" in out


class TestForceWarning:
    def _seed_parquet(self, tmp_path: Path, dataset_id: str) -> Path:
        path = tmp_path / "ECB" / "series" / f"{dataset_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
        return path

    def test_force_logs_warning_with_count(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        # -d skips listing, so no list_datasets patch needed.
        self._seed_parquet(tmp_path, "YC")
        with patch("parsimony_sdmx.cli.main.run_agency", return_value=[
            DatasetOutcome(
                dataset_id="YC", agency_id="ECB", status=OutcomeStatus.OK
            ),
        ]) as run_agency:
            rc = main(
                ["-a", "ECB", "-d", "YC", "--force", "-o", str(tmp_path)]
            )
        assert rc == 0
        run_agency.assert_called_once()
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        msgs = "\n".join(r.getMessage() for r in warnings)
        assert "--force" in msgs
        assert "1 existing" in msgs

    def test_force_without_existing_parquet_no_warning(
        self, caplog: pytest.LogCaptureFixture, tmp_path: Path
    ) -> None:
        with patch("parsimony_sdmx.cli.main.run_agency", return_value=[
            DatasetOutcome(
                dataset_id="YC", agency_id="ECB", status=OutcomeStatus.OK
            ),
        ]) as run_agency:
            rc = main(
                ["-a", "ECB", "-d", "YC", "--force", "-o", str(tmp_path)]
            )
        assert rc == 0
        run_agency.assert_called_once()
        force_warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING" and "--force" in r.getMessage()
        ]
        assert force_warnings == []


class TestMainDispatch:
    def test_all_writes_datasets_parquet_then_runs_agency(
        self, tmp_path: Path
    ) -> None:
        recs = _recs(("YC", "t"), ("CPI", "t"))
        outcomes = [
            DatasetOutcome(
                dataset_id="YC", agency_id="ECB", status=OutcomeStatus.OK
            ),
            DatasetOutcome(
                dataset_id="CPI",
                agency_id="ECB",
                status=OutcomeStatus.FAILED,
                kind=FailureKind.TIMEOUT,
            ),
        ]
        with (
            patch("parsimony_sdmx.cli.main.list_datasets", return_value=recs),
            patch("parsimony_sdmx.cli.main.run_agency", return_value=outcomes),
        ):
            rc = main(["-a", "ECB", "--all", "-o", str(tmp_path)])
        # Any FAILED → 1.
        assert rc == 1
        assert (tmp_path / "ECB" / "datasets.parquet").exists()

    def test_single_dataset_failure_exit_1(self, tmp_path: Path) -> None:
        outcomes = [
            DatasetOutcome(
                dataset_id="YC",
                agency_id="ECB",
                status=OutcomeStatus.FAILED,
                kind=FailureKind.HTTP_ERROR,
            )
        ]
        # No listing patch needed — -d bypasses it.
        with patch(
            "parsimony_sdmx.cli.main.run_agency", return_value=outcomes
        ):
            rc = main(["-a", "ECB", "-d", "YC", "-o", str(tmp_path)])
        assert rc == 1
