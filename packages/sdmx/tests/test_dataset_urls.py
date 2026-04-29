from parsimony_sdmx.providers.dataset_urls import build_sdmx_dataset_url


class TestBuildSdmxDatasetUrl:
    def test_ecb_url(self) -> None:
        out = build_sdmx_dataset_url("ECB", "YC")
        assert out == "https://data.ecb.europa.eu/data/datasets/YC"

    def test_ecb_url_case_insensitive_agency(self) -> None:
        out = build_sdmx_dataset_url("ecb", "YC")
        assert out == "https://data.ecb.europa.eu/data/datasets/YC"

    def test_estat_url(self) -> None:
        out = build_sdmx_dataset_url("ESTAT", "PRC_HICP_MIDX")
        assert out == (
            "https://ec.europa.eu/eurostat/databrowser/view/"
            "PRC_HICP_MIDX/default/table?lang=en"
        )

    def test_imf_data_url(self) -> None:
        out = build_sdmx_dataset_url("IMF_DATA", "IFS")
        assert out == "https://data.imf.org/?sk=IFS"

    def test_imf_alias_url(self) -> None:
        out = build_sdmx_dataset_url("IMF", "IFS")
        assert out == "https://data.imf.org/?sk=IFS"

    def test_wb_wdi_returns_none(self) -> None:
        assert build_sdmx_dataset_url("WB_WDI", "WDI") is None

    def test_unknown_agency_returns_none(self) -> None:
        assert build_sdmx_dataset_url("UNKNOWN", "X") is None

    def test_dataset_id_is_percent_encoded(self) -> None:
        out = build_sdmx_dataset_url("ECB", "DSD/foo bar")
        assert out is not None
        assert "DSD%2Ffoo%20bar" in out
