from parsimony_sdmx.cli.summary import format_summary
from parsimony_sdmx.core.outcomes import DatasetOutcome, FailureKind, OutcomeStatus


def _ok(ds: str, rows: int = 10) -> DatasetOutcome:
    return DatasetOutcome(
        dataset_id=ds,
        agency_id="ECB",
        status=OutcomeStatus.OK,
        rows=rows,
        bytes=4096,
        duration_s=1.5,
    )


def _failed(ds: str, kind: FailureKind = FailureKind.TIMEOUT) -> DatasetOutcome:
    return DatasetOutcome(
        dataset_id=ds,
        agency_id="ECB",
        status=OutcomeStatus.FAILED,
        kind=kind,
        error_message="upstream timed out",
    )


def _empty(ds: str) -> DatasetOutcome:
    return DatasetOutcome(
        dataset_id=ds,
        agency_id="ECB",
        status=OutcomeStatus.EMPTY,
        rows=0,
    )


class TestFormatSummary:
    def test_empty_input(self) -> None:
        assert format_summary([]) == "No datasets processed."

    def test_all_ok(self) -> None:
        out = format_summary([_ok("A"), _ok("B"), _ok("C")])
        assert "ok:     3" in out
        assert "failed: 0" in out
        assert "Failed datasets" not in out

    def test_mixed(self) -> None:
        out = format_summary(
            [_ok("A"), _empty("B"), _failed("C"), _failed("D", FailureKind.HTTP_ERROR)]
        )
        assert "ok:     1" in out
        assert "empty:  1" in out
        assert "failed: 2" in out
        assert "Failed datasets:" in out
        assert "C [timeout]" in out
        assert "D [http_error]" in out

    def test_failures_sorted_by_id(self) -> None:
        out = format_summary([_failed("Z"), _failed("A"), _failed("M")])
        a_idx = out.index("A [")
        m_idx = out.index("M [")
        z_idx = out.index("Z [")
        assert a_idx < m_idx < z_idx

    def test_multiline_error_truncated(self) -> None:
        o = DatasetOutcome(
            dataset_id="X",
            agency_id="ECB",
            status=OutcomeStatus.FAILED,
            kind=FailureKind.UNKNOWN,
            error_message="first line\nsecond line that must not appear",
        )
        out = format_summary([o])
        assert "first line" in out
        assert "second line" not in out

    def test_failed_without_error_message(self) -> None:
        o = DatasetOutcome(
            dataset_id="X",
            agency_id="ECB",
            status=OutcomeStatus.FAILED,
            kind=FailureKind.UNKNOWN,
        )
        out = format_summary([o])
        assert "X [unknown]" in out
