"""Agency-scoped output paths.

Every runtime artifact (structure JSON, tmp files) lives under
``outputs/{AGENCY}/`` so a single run's state is isolated and the orphan
sweep can operate per-agency.
"""

from __future__ import annotations

from pathlib import Path

from parsimony_sdmx.io.paths import safe_filename

TMP_DIR = ".tmp"
DATASETS_PARQUET = "datasets.parquet"


def agency_dir(output_base: Path, agency_id: str) -> Path:
    return output_base / safe_filename(agency_id)


def datasets_parquet(output_base: Path, agency_id: str) -> Path:
    return agency_dir(output_base, agency_id) / DATASETS_PARQUET


def structure_json(output_base: Path, agency_id: str, dataset_id: str) -> Path:
    return agency_dir(output_base, agency_id) / "structure" / f"{safe_filename(dataset_id)}.json"


def tmp_dir(output_base: Path, agency_id: str) -> Path:
    return agency_dir(output_base, agency_id) / TMP_DIR
