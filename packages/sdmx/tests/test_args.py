from pathlib import Path

import pytest

from parsimony_sdmx.cli.args import parse_args


class TestParseArgs:
    def test_single_dataset(self) -> None:
        cfg = parse_args(["-a", "ECB", "-d", "YC"])
        assert cfg.agency_id == "ECB"
        assert cfg.dataset_id == "YC"
        assert cfg.all_datasets is False
        assert cfg.list_datasets is False
        assert cfg.catalog_only is False
        assert cfg.dry_run is False
        assert cfg.force is False

    def test_catalog(self) -> None:
        cfg = parse_args(["-a", "ECB", "--catalog"])
        assert cfg.catalog_only is True
        assert cfg.dataset_id is None
        assert cfg.all_datasets is False

    def test_catalog_mutually_exclusive_with_dataset(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["-a", "ECB", "-d", "YC", "--catalog"])

    def test_all(self) -> None:
        cfg = parse_args(["-a", "ECB", "--all"])
        assert cfg.all_datasets is True
        assert cfg.dataset_id is None

    def test_list_datasets(self) -> None:
        cfg = parse_args(["-a", "ESTAT", "--list-datasets"])
        assert cfg.list_datasets is True

    def test_dry_run_with_all(self) -> None:
        cfg = parse_args(["-a", "ECB", "--all", "--dry-run"])
        assert cfg.dry_run is True
        assert cfg.all_datasets is True

    def test_force(self) -> None:
        cfg = parse_args(["-a", "ECB", "-d", "YC", "--force"])
        assert cfg.force is True

    def test_output_dir_default(self) -> None:
        cfg = parse_args(["-a", "ECB", "-d", "YC"])
        assert cfg.output_base == Path("outputs").resolve()

    def test_output_dir_custom(self, tmp_path: Path) -> None:
        target = str(tmp_path / "custom_out")
        cfg = parse_args(["-a", "ECB", "-d", "YC", "-o", target])
        assert cfg.output_base == Path(target).resolve()

    def test_verbose(self) -> None:
        cfg = parse_args(["-a", "ECB", "-d", "YC", "-v"])
        assert cfg.verbose is True

    def test_dataset_timeout_default_15min(self) -> None:
        cfg = parse_args(["-a", "ECB", "-d", "YC"])
        assert cfg.dataset_timeout_s == 900.0

    def test_dataset_timeout_custom(self) -> None:
        cfg = parse_args(["-a", "ECB", "-d", "YC", "--dataset-timeout", "60"])
        assert cfg.dataset_timeout_s == 60.0

    def test_dataset_timeout_zero_disables(self) -> None:
        # Zero (or negative) means unbounded — the orchestrator default becomes None.
        cfg = parse_args(["-a", "ECB", "-d", "YC", "--dataset-timeout", "0"])
        assert cfg.dataset_timeout_s is None

    def test_agency_required(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["-d", "YC"])

    def test_unknown_agency_rejected(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["-a", "NOPE", "-d", "YC"])

    def test_mode_group_required(self) -> None:
        # No --dataset, --all, or --list-datasets → error.
        with pytest.raises(SystemExit):
            parse_args(["-a", "ECB"])

    def test_mode_group_mutually_exclusive(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["-a", "ECB", "-d", "YC", "--all"])
