import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from parsimony_sdmx._isolation.layout import oom_dir
from parsimony_sdmx._isolation.memory_monitor import (
    MemoryMonitorConfig,
    _kill_largest_child,
    clear_worker_marker,
    memory_monitor,
    read_oom_marker,
    write_worker_marker,
)


class TestWorkerMarker:
    def test_write_read_clear_round_trip(self, tmp_path: Path) -> None:
        path = write_worker_marker(tmp_path, "ECB", 12345, "YC", phase="running")
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["dataset_id"] == "YC"
        assert data["pid"] == 12345
        assert data["phase"] == "running"
        assert "started_at" in data

        clear_worker_marker(tmp_path, "ECB", 12345)
        assert not path.exists()

    def test_clear_missing_is_noop(self, tmp_path: Path) -> None:
        clear_worker_marker(tmp_path, "ECB", 99999)  # no error


class TestReadOomMarker:
    def test_missing_returns_none(self, tmp_path: Path) -> None:
        assert read_oom_marker(tmp_path, "ECB", 123) is None

    def test_reads_json(self, tmp_path: Path) -> None:
        d = oom_dir(tmp_path, "ECB")
        d.mkdir(parents=True)
        (d / "123.json").write_text(
            json.dumps({"pid": 123, "rss_bytes": 100, "dataset_id": "YC"})
        )
        got = read_oom_marker(tmp_path, "ECB", 123)
        assert got is not None
        assert got["dataset_id"] == "YC"

    def test_unreadable_returns_none(self, tmp_path: Path) -> None:
        d = oom_dir(tmp_path, "ECB")
        d.mkdir(parents=True)
        (d / "123.json").write_text("not json")
        assert read_oom_marker(tmp_path, "ECB", 123) is None


class TestKillLargestChild:
    def _fake_child(self, pid: int, rss: int) -> MagicMock:
        child = MagicMock()
        child.pid = pid
        info = MagicMock()
        info.rss = rss
        child.memory_info.return_value = info
        return child

    def test_writes_oom_marker_with_worker_data_before_kill(
        self, tmp_path: Path
    ) -> None:
        # Worker 501 registered itself with dataset_id.
        write_worker_marker(tmp_path, "ECB", 501, "YC", phase="fetching")

        parent = MagicMock()
        parent.children.return_value = [
            self._fake_child(500, 100 * 1024 * 1024),
            self._fake_child(501, 500 * 1024 * 1024),  # largest
            self._fake_child(502, 200 * 1024 * 1024),
        ]

        killed = _kill_largest_child(parent, tmp_path, "ECB", system_percent=92.0)
        assert killed is True

        # The PID=501 child should have been killed.
        parent.children.return_value[1].send_signal.assert_called_once()

        # OOM marker written with peak RSS + dataset_id from worker marker.
        marker = read_oom_marker(tmp_path, "ECB", 501)
        assert marker is not None
        assert marker["pid"] == 501
        assert marker["rss_bytes"] == 500 * 1024 * 1024
        assert marker["system_memory_percent"] == 92.0
        assert marker["dataset_id"] == "YC"
        assert marker["phase"] == "fetching"

    def test_no_children_returns_false(self, tmp_path: Path) -> None:
        parent = MagicMock()
        parent.children.return_value = []
        assert _kill_largest_child(parent, tmp_path, "ECB", 95.0) is False


class TestMemoryMonitorContext:
    def test_enters_and_exits_cleanly_below_threshold(
        self, tmp_path: Path
    ) -> None:
        cfg = MemoryMonitorConfig(threshold_percent=99.9, poll_seconds=0.05)
        fake_mem = MagicMock(percent=10.0)
        with patch("psutil.virtual_memory", return_value=fake_mem), memory_monitor(tmp_path, "ECB", cfg):
            time.sleep(0.15)
        # No OOM markers should exist.
        d = oom_dir(tmp_path, "ECB")
        if d.exists():
            assert [p for p in d.iterdir() if p.name.endswith(".json")] == [
                p for p in d.iterdir() if p.name.startswith("worker.")
            ]
