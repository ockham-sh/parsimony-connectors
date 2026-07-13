"""Unit tests for ``sdmx_fetch`` — param validation + error taxonomy.

Live-endpoint tests live under the ``e2e`` marker and run with
``SDMX_RUN_E2E=1``; this module does not hit any network.
"""

from __future__ import annotations

from typing import Any, Literal

import pytest
from parsimony.errors import EmptyDataError, InvalidParameterError
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
        with pytest.raises(InvalidParameterError, match="must include agency prefix"):
            SdmxFetchParams(dataset_key="FOO123", series_key="B.U2.EUR")

    def test_rejects_unknown_agency(self) -> None:
        with pytest.raises(InvalidParameterError, match="Unknown agency"):
            SdmxFetchParams(dataset_key="OECD-MEI", series_key="B.U2.EUR")

    def test_rejects_path_traversal_in_dataset_id(self) -> None:
        with pytest.raises(InvalidParameterError, match="disallowed characters"):
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


class TestEmptyDocumentClassification:
    """A no-data 200 (empty body -> lxml 'no element found') must be EmptyDataError.

    Observed live on ECB-BOP with a period range past a discontinued series' coverage:
    the old classification called it a "transient fetch error ... Retry shortly", which
    is deterministic and misleading (issue field-tested 2026-07-09).
    """

    def test_empty_document_maps_to_empty_data_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from parsimony_sdmx.connectors import fetch as fetch_mod

        # Same type name + message shape lxml raises; matched by name, so no lxml import.
        xml_error = type("XMLSyntaxError", (Exception,), {})("no element found (line 0)")

        class _FakeClient:
            def __enter__(self) -> _FakeClient:
                return self

            def __exit__(self, *args: object) -> Literal[False]:
                return False

            def get(self, **kwargs: object) -> None:
                raise xml_error

        monkeypatch.setattr(
            "parsimony_sdmx.providers.sdmx_client.sdmx_client",
            lambda agency_id, wb_url_rewrite=True: _FakeClient(),
        )
        monkeypatch.setattr(fetch_mod, "_resolve_structure", lambda dataset_key: None)

        with pytest.raises(EmptyDataError) as exc:
            fetch_mod.sdmx_fetch(
                dataset_ref="ECB-BOP",
                series_ref="Q.U2.N.4.993.N.A1.E",
                start_period="2023-Q1",
                end_period="2025-Q4",
            )
        msg = str(exc.value)
        assert "empty document" in msg
        assert "start_period" in msg
        assert "Retry shortly" not in msg

    def test_other_parse_errors_stay_provider_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A genuinely malformed (non-empty) document is NOT reclassified."""
        from parsimony.errors import ProviderError

        from parsimony_sdmx.connectors import fetch as fetch_mod

        xml_error = type("XMLSyntaxError", (Exception,), {})("Opening and ending tag mismatch (line 7)")

        class _FakeClient:
            def __enter__(self) -> _FakeClient:
                return self

            def __exit__(self, *args: object) -> Literal[False]:
                return False

            def get(self, **kwargs: object) -> None:
                raise xml_error

        monkeypatch.setattr(
            "parsimony_sdmx.providers.sdmx_client.sdmx_client",
            lambda agency_id, wb_url_rewrite=True: _FakeClient(),
        )
        monkeypatch.setattr(fetch_mod, "_resolve_structure", lambda dataset_key: None)

        with pytest.raises(ProviderError):
            fetch_mod.sdmx_fetch(dataset_ref="ECB-BOP", series_ref="Q.U2.N.4.993.N.A1.E")


class TestOrGroupCoverage:
    """A '+'-OR'd code that contributed zero rows must raise, not silently vanish.

    Observed live: UK dropped from an EL+TR+IS+UK Eurostat HICP pull with no signal
    (UK stopped reporting to Eurostat after 2020), contradicting the docstring's
    none-dropped promise.
    """

    @staticmethod
    def _run_fetch(monkeypatch: pytest.MonkeyPatch, fake_df: Any, series_ref: str) -> Any:
        from parsimony_sdmx.connectors import fetch as fetch_mod

        monkeypatch.setattr("sdmx.to_pandas", lambda data, attributes=None: fake_df.copy())

        class _FakeClient:
            def __enter__(self) -> object:
                return self

            def __exit__(self, *args: object) -> Literal[False]:
                return False

            def get(self, **kwargs: object) -> object:
                return type("Msg", (), {"data": None})()

        monkeypatch.setattr(
            "parsimony_sdmx.providers.sdmx_client.sdmx_client",
            lambda agency_id, wb_url_rewrite=True: _FakeClient(),
        )
        structure = fetch_mod._ResolvedStructure(
            dataset_id="TEST",
            dsd=None,
            structure_msg=None,
            dsd_dim_ids=["FREQ", "geo"],
            label_maps={},
        )
        monkeypatch.setattr(fetch_mod, "_resolve_structure", lambda dataset_key: structure)
        return fetch_mod.sdmx_fetch(
            dataset_ref="ESTAT-TEST", series_ref=series_ref, start_period="2024-01", end_period="2025-12"
        )

    def test_missing_or_member_raises_naming_dimension_and_codes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pandas as pd

        fake = pd.DataFrame(
            {
                "FREQ": ["M", "M", "M"],
                "geo": ["EL", "TR", "IS"],  # UK requested but absent
                "TIME_PERIOD": ["2024-01"] * 3,
                "value": [1.0, 2.0, 3.0],
            }
        )
        with pytest.raises(EmptyDataError) as exc:
            self._run_fetch(monkeypatch, fake, series_ref="M.EL+TR+IS+UK")
        msg = str(exc.value)
        assert "geo: ['UK']" in msg
        assert "widen start_period/end_period" in msg

    def test_all_or_members_present_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pandas as pd

        fake = pd.DataFrame(
            {
                "FREQ": ["M", "M"],
                "geo": ["EL", "TR"],
                "TIME_PERIOD": ["2024-01"] * 2,
                "value": [1.0, 2.0],
            }
        )
        result = self._run_fetch(monkeypatch, fake, series_ref="M.EL+TR")
        assert set(result.data["value"]) == {1.0, 2.0}

    def test_partial_key_shape_is_left_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A key with fewer segments than the DSD (provider-permitting) skips the check."""
        import pandas as pd

        fake = pd.DataFrame(
            {
                "FREQ": ["M"],
                "geo": ["EL"],
                "TIME_PERIOD": ["2024-01"],
                "value": [1.0],
            }
        )
        result = self._run_fetch(monkeypatch, fake, series_ref="EL+UK")
        assert len(result.data) == 1


class TestUnitAttributePassthrough:
    """UNIT / UNIT_MULT series attributes surface as labeled METADATA columns; other
    attributes (OBS_STATUS, ...) stay dropped and value stays numeric."""

    def test_unit_columns_surface_labeled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pandas as pd

        from parsimony_sdmx.connectors import fetch as fetch_mod

        fake = pd.DataFrame(
            {
                "FREQ": ["M"],
                "REF_AREA": ["DE"],
                "TIME_PERIOD": ["2024-01"],
                "value": ["3.2"],  # object dtype, as sdmx1 can deliver with attributes
                "UNIT": ["PC"],
                "UNIT_MULT": ["0"],
                "OBS_STATUS": ["A"],
            }
        )
        monkeypatch.setattr("sdmx.to_pandas", lambda data, attributes=None: fake.copy())

        class _FakeClient:
            def __enter__(self) -> _FakeClient:
                return self

            def __exit__(self, *args: object) -> Literal[False]:
                return False

            def get(self, **kwargs: object) -> object:
                return type("Msg", (), {"data": None})()

        monkeypatch.setattr(
            "parsimony_sdmx.providers.sdmx_client.sdmx_client",
            lambda agency_id, wb_url_rewrite=True: _FakeClient(),
        )
        structure = fetch_mod._ResolvedStructure(
            dataset_id="TEST",
            dsd=None,
            structure_msg=None,
            dsd_dim_ids=["FREQ", "REF_AREA"],
            label_maps={"FREQ": {"M": "Monthly"}, "REF_AREA": {"DE": "Germany"}, "UNIT": {"PC": "Percent"}},
        )
        monkeypatch.setattr(fetch_mod, "_resolve_structure", lambda dataset_key: structure)

        df = fetch_mod.sdmx_fetch(dataset_ref="ECB-TEST", series_ref="M.DE").data

        # Dimensions are code-only (their labels ride in `title`); UNIT / UNIT_MULT keep
        # a label because it qualifies `value` and is not carried by the title.
        assert {"UNIT_code", "UNIT_label", "UNIT_MULT_code", "UNIT_MULT_label"} <= set(df.columns)
        assert "UNIT" not in df.columns
        assert "FREQ_label" not in df.columns and "REF_AREA_label" not in df.columns
        assert "OBS_STATUS" not in df.columns and "OBS_STATUS_code" not in df.columns
        assert df["UNIT_code"].iloc[0] == "PC"
        assert df["UNIT_label"].iloc[0] == "Percent"  # labeled via the codelist map
        assert df["value"].dtype.kind == "f"  # object "3.2" coerced to numeric
        assert df["value"].iloc[0] == pytest.approx(3.2)
