"""End-to-end structured search tests for sdmx_series_search."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from parsimony.catalog import Catalog, CatalogEntry

from parsimony_sdmx.catalog_policy import sdmx_series_entries, sdmx_series_indexes
from parsimony_sdmx.connectors.search import sdmx_series_search


def _entries() -> list[CatalogEntry]:
    return [
        CatalogEntry(
            namespace="sdmx_series_ecb_yc",
            code="A.U2.SR_10Y",
            title="ECB source title",
            metadata={
                "FREQ_code": "A",
                "FREQ_label": "Annual",
                "REF_AREA_code": "U2",
                "REF_AREA_label": "Euro area",
                "DATA_TYPE_FM_code": "SR_10Y",
                "DATA_TYPE_FM_label": "Yield curve spot rate, 10-year maturity",
                "source_title": "ECB source title",
            },
        ),
        CatalogEntry(
            namespace="sdmx_series_ecb_yc",
            code="M.DE.IF_1Y",
            title="Synthetic title",
            metadata={
                "FREQ_code": "M",
                "FREQ_label": "Monthly",
                "REF_AREA_code": "DE",
                "REF_AREA_label": "Germany",
                "DATA_TYPE_FM_code": "IF_1Y",
                "DATA_TYPE_FM_label": "Instantaneous forward rate, 1-year maturity",
                "EXTRA_label": "Extra dimension label",
                "EXTRA_code": "X",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_search_structured_end_to_end() -> None:
    raw = _entries()
    dims = ["FREQ", "REF_AREA", "DATA_TYPE_FM"]
    entries = sdmx_series_entries(raw, dims)

    cat = Catalog("sdmx_series_ecb_yc")
    cat.set_indexes(sdmx_series_indexes(entries, dims))
    cat.set_entries(entries)
    await cat.build()

    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "sdmx_series_ecb_yc"
        await cat.save(path)

        res_df = await sdmx_series_search(
            query="REF_AREA: Germany && FREQ: Monthly",
            flow_id="ECB/YC",
            limit=5,
            catalog_root=f"file://{tmpdir}",
        )

        df = res_df.df
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.loc[0, "series_key"] == "M.DE.IF_1Y"
        assert "Synthetic title" in df.loc[0, "title"]
