"""Agency-specific dataset portal URLs for fetched series provenance."""

from __future__ import annotations

from urllib.parse import quote


def build_sdmx_dataset_url(agency_id: str, dataset_id: str) -> str | None:
    """Return the human-facing portal URL for ``dataset_id`` at ``agency_id``.

    Returns None when the agency has no known portal (e.g. ``WB_WDI``).
    The dataset_id is percent-encoded so that downstream URLs survive
    SDMX identifiers that contain reserved URL characters.
    """
    normalized = agency_id.upper()
    encoded = quote(dataset_id, safe="")
    if normalized == "ECB":
        return f"https://data.ecb.europa.eu/data/datasets/{encoded}"
    if normalized == "ESTAT":
        return f"https://ec.europa.eu/eurostat/databrowser/view/{encoded}/default/table?lang=en"
    if normalized in ("IMF", "IMF_DATA"):
        return f"https://data.imf.org/?sk={encoded}"
    return None
