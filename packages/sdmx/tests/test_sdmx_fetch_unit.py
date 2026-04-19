"""Unit tests for ``sdmx_fetch`` — param validation + error taxonomy.

Live-endpoint tests live under the ``e2e`` marker and run with
``SDMX_RUN_E2E=1``; this module does not hit any network.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from parsimony_sdmx.connectors.fetch import SdmxFetchParams


class TestParamValidation:
    def test_accepts_valid_dataset_and_series_key(self) -> None:
        p = SdmxFetchParams(
            dataset_key="ECB-YC",
            series_key="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
            start_period="2020-01",
            end_period="2024-12",
        )
        assert p.dataset_key == "ECB-YC"

    def test_normalizes_agency_prefix_to_uppercase(self) -> None:
        p = SdmxFetchParams(dataset_key="ecb-YC", series_key="B.U2.EUR")
        assert p.dataset_key == "ECB-YC"

    def test_rejects_missing_agency_prefix(self) -> None:
        with pytest.raises(ValidationError, match="must include agency prefix"):
            SdmxFetchParams(dataset_key="FOO123", series_key="B.U2.EUR")

    def test_rejects_unknown_agency(self) -> None:
        with pytest.raises(ValidationError, match="Unknown agency"):
            SdmxFetchParams(dataset_key="OECD-MEI", series_key="B.U2.EUR")

    def test_rejects_path_traversal_in_dataset_id(self) -> None:
        with pytest.raises(ValidationError, match="disallowed characters"):
            SdmxFetchParams(dataset_key="ECB-../secret", series_key="B.U2.EUR")

    def test_rejects_whitespace_in_series_key(self) -> None:
        with pytest.raises(ValidationError):
            SdmxFetchParams(dataset_key="ECB-YC", series_key="B.U2 EUR")

    def test_rejects_query_smuggling_in_series_key(self) -> None:
        with pytest.raises(ValidationError):
            SdmxFetchParams(dataset_key="ECB-YC", series_key="B.U2?token=x")

    def test_series_key_length_bounded(self) -> None:
        with pytest.raises(ValidationError):
            SdmxFetchParams(dataset_key="ECB-YC", series_key="A" * 300)
