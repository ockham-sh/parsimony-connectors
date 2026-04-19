import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.providers.ecb_portal import (
    _cache_path,
    _parse_portal_listing,
    _sanitise_value,
    default_cache_dir,
    scrape_ecb_portal,
)

SAMPLE_HTML = b"""
<html><body>
  <div class="expandable-item">
    <a href="/data/datasets/YC">Yield curve</a>
    <div class="expandable-description">Yield curve parameters</div>
  </div>
  <div class="expandable-item">
    <a href="/data/datasets/MIR">Interest rates</a>
    <div class="expandable-description">Monetary financial institutions</div>
  </div>
  <div class="expandable-item">
    <a href="/data/datasets/BAD$ID">Bad</a>
    <div class="expandable-description">skipped</div>
  </div>
</body></html>
"""


class TestParsePortalListing:
    def test_extracts_valid_datasets(self) -> None:
        out = _parse_portal_listing(SAMPLE_HTML)
        assert "YC" in out
        assert "MIR" in out
        assert "Yield curve" in out["YC"]
        assert "parameters" in out["YC"]

    def test_skips_invalid_dataset_ids(self) -> None:
        out = _parse_portal_listing(SAMPLE_HTML)
        assert "BAD$ID" not in out

    def test_empty_html_yields_empty_dict(self) -> None:
        assert _parse_portal_listing(b"<html></html>") == {}


class TestCache:
    def test_cache_path_includes_date_and_url_hash(self, tmp_path: Path) -> None:
        p = _cache_path(tmp_path, "https://example.com/a", date(2026, 4, 17))
        assert "2026-04-17" in p.name
        # URL hash differentiates caches per URL
        p2 = _cache_path(tmp_path, "https://example.com/b", date(2026, 4, 17))
        assert p.name != p2.name

    def test_scrape_writes_and_reads_cache(self, tmp_path: Path) -> None:
        fake_session = object()
        with patch(
            "parsimony_sdmx.providers.ecb_portal.bounded_get",
            return_value=SAMPLE_HTML,
        ) as mock_get:
            out1 = scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=tmp_path,
                today=date(2026, 4, 17),
            )
            assert "YC" in out1
            assert mock_get.call_count == 1

            # Second call — cache hit, no HTTP.
            out2 = scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=tmp_path,
                today=date(2026, 4, 17),
            )
            assert out2 == out1
            assert mock_get.call_count == 1

    def test_different_day_refetches(self, tmp_path: Path) -> None:
        fake_session = object()
        with patch(
            "parsimony_sdmx.providers.ecb_portal.bounded_get",
            return_value=SAMPLE_HTML,
        ) as mock_get:
            scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=tmp_path,
                today=date(2026, 4, 17),
            )
            scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=tmp_path,
                today=date(2026, 4, 18),
            )
            assert mock_get.call_count == 2

    def test_default_cache_dir_uses_xdg(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        expected = tmp_path / "parsimony-sdmx" / "ecb-portal"
        assert default_cache_dir() == expected


class TestCacheHardening:
    def test_world_writable_cache_dir_rejected(self, tmp_path: Path) -> None:
        bad_dir = tmp_path / "shared"
        bad_dir.mkdir()
        os.chmod(bad_dir, 0o777)  # noqa: S103 - intentional for test
        fake_session = object()
        with pytest.raises(SdmxFetchError, match="writable"):
            scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=bad_dir,
                today=date(2026, 4, 17),
            )

    def test_group_writable_cache_dir_rejected(self, tmp_path: Path) -> None:
        bad_dir = tmp_path / "group"
        bad_dir.mkdir()
        os.chmod(bad_dir, 0o775)  # noqa: S103 - intentional for test
        fake_session = object()
        with pytest.raises(SdmxFetchError, match="writable"):
            scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=bad_dir,
                today=date(2026, 4, 17),
            )

    def test_poisoned_cache_values_are_sanitised(self, tmp_path: Path) -> None:
        # An attacker drops a cache file with control chars + huge values.
        os.chmod(tmp_path, 0o700)
        cache_path = _cache_path(tmp_path, "https://example.com", date(2026, 4, 17))
        poisoned = {
            "YC": "Clean description",
            "MIR": "evil\x1b[2Jstring\x00with\r\nnewlines",
            "BAD$ID": "invalid id must be dropped",
            "BIG": "x" * 100_000,
        }
        cache_path.write_text(json.dumps(poisoned), encoding="utf-8")
        fake_session = object()
        with patch("parsimony_sdmx.providers.ecb_portal.bounded_get") as mock_get:
            out = scrape_ecb_portal(
                fake_session,  # type: ignore[arg-type]
                cache_dir=tmp_path,
                today=date(2026, 4, 17),
                url="https://example.com",
            )
            # No HTTP call because cache exists (even if some entries are rejected).
            mock_get.assert_not_called()
        assert "YC" in out
        assert out["YC"] == "Clean description"
        assert "BAD$ID" not in out  # invalid id rejected by validate_sdmx_id
        assert "\x1b" not in out["MIR"]
        assert "\x00" not in out["MIR"]
        assert "\r" not in out["MIR"]
        assert "\n" not in out["MIR"]
        assert len(out["BIG"]) <= 2048  # length cap


class TestSanitiseValue:
    def test_strips_control_chars(self) -> None:
        assert _sanitise_value("a\x00b\x01c") == "a b c"

    def test_caps_length(self) -> None:
        out = _sanitise_value("x" * 5000)
        assert len(out) <= 2048

    def test_collapses_whitespace(self) -> None:
        assert _sanitise_value("a    b\t\t c") == "a b c"
