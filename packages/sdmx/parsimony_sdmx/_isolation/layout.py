"""Agency-scoped output paths.

Every runtime artifact (structure JSON) lives under ``outputs/{AGENCY}/`` so a
single run's state is isolated and the orphan sweep can operate per-agency.
"""

from __future__ import annotations

from pathlib import Path

from parsimony_sdmx.io.paths import safe_filename


def agency_dir(output_base: Path, agency_id: str) -> Path:
    return output_base / safe_filename(agency_id)


def structure_json(output_base: Path, agency_id: str, dataset_id: str) -> Path:
    return agency_dir(output_base, agency_id) / "structure" / f"{safe_filename(dataset_id)}.json"
