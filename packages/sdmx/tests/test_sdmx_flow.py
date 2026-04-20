from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import DatasetRecord, SeriesRecord
from parsimony_sdmx.providers.sdmx_flow import list_datasets_flow, list_series_flow


def _named(locs: dict[str, str]) -> SimpleNamespace:
    return SimpleNamespace(localizations=locs)


def _flow(flow_id: str, name: str, desc: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        id=flow_id,
        name=_named({"en": name}),
        description=_named({"en": desc} if desc else {}),
    )


def _code(cid: str, label: str) -> SimpleNamespace:
    return SimpleNamespace(id=cid, name=_named({"en": label}))


def _dim(dim_id: str, codelist_id: str | None = None) -> SimpleNamespace:
    local = SimpleNamespace(enumerated=SimpleNamespace(id=codelist_id)) if codelist_id else None
    return SimpleNamespace(
        id=dim_id, local_representation=local, concept_identity=None
    )


def _sk(values: dict[str, str]) -> SimpleNamespace:
    kvs = {d: SimpleNamespace(id=d, value=v) for d, v in values.items()}
    return SimpleNamespace(values=kvs)


class TestListDatasetsFlow:
    def test_yields_dataset_records(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = SimpleNamespace(
            dataflow={
                "YC": _flow("YC", "Yield Curve", "curve params"),
                "CPI": _flow("CPI", "Consumer Price Index"),
            }
        )
        out = list(list_datasets_flow(client, "ECB"))
        assert out == [
            DatasetRecord(
                dataset_id="YC",
                agency_id="ECB",
                title="Yield Curve - curve params",
            ),
            DatasetRecord(
                dataset_id="CPI",
                agency_id="ECB",
                title="Consumer Price Index",
            ),
        ]

    def test_decorate_title_hook(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = SimpleNamespace(
            dataflow={"WDI": _flow("WDI", "World Development Indicators")}
        )
        out = list(
            list_datasets_flow(
                client, "WB_WDI", decorate_title=lambda fid, t: f"World Bank - {t}"
            )
        )
        assert out[0].title == "World Bank - World Development Indicators"

    def test_client_exception_wrapped(self) -> None:
        client = MagicMock()
        client.dataflow.side_effect = RuntimeError("network")
        with pytest.raises(SdmxFetchError, match="Failed to list dataflows"):
            list(list_datasets_flow(client, "ECB"))

    def test_empty_dataflow_map_yields_nothing(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = SimpleNamespace(dataflow={})
        assert list(list_datasets_flow(client, "ECB")) == []


class TestListSeriesFlow:
    def _build_msg(
        self,
        dataset_id: str,
        dsd_id: str = "DSD_YC",
        dims: list[tuple[str, str | None]] | None = None,
        codelists: dict[str, list[SimpleNamespace]] | None = None,
    ) -> SimpleNamespace:
        dims = dims or [("FREQ", "CL_FREQ"), ("REF_AREA", "CL_AREA"), ("TIME_PERIOD", None)]
        codelists = codelists or {
            "CL_FREQ": [_code("A", "Annual"), _code("M", "Monthly")],
            "CL_AREA": [_code("U2", "Euro area")],
        }
        flow = SimpleNamespace(
            id=dataset_id,
            name=_named({"en": "dummy"}),
            description=_named({}),
            structure=SimpleNamespace(id=dsd_id),
        )
        dsd = SimpleNamespace(
            id=dsd_id,
            dimensions=[_dim(did, cid) for did, cid in dims],
        )
        return SimpleNamespace(
            dataflow={dataset_id: flow},
            structure={dsd_id: dsd},
            codelist=codelists,
        )

    def test_yields_series_records(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = self._build_msg("YC")
        client.series_keys.return_value = [
            _sk({"FREQ": "A", "REF_AREA": "U2"}),
            _sk({"FREQ": "M", "REF_AREA": "U2"}),
        ]
        out = list(list_series_flow(client, "ECB", "YC"))
        assert out == [
            SeriesRecord(id="A.U2", dataset_id="YC", title="A: Annual - U2: Euro area"),
            SeriesRecord(id="M.U2", dataset_id="YC", title="M: Monthly - U2: Euro area"),
        ]

    def test_augment_hook_applied(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = self._build_msg("YC")
        client.series_keys.return_value = [_sk({"FREQ": "A", "REF_AREA": "U2"})]
        out = list(
            list_series_flow(
                client,
                "ECB",
                "YC",
                augment=lambda base, sid: f"{base} | ECB:{sid}",
            )
        )
        assert out[0].title == "A: Annual - U2: Euro area | ECB:A.U2"

    def test_dsd_fetched_separately_if_not_in_msg(self) -> None:
        client = MagicMock()
        msg = self._build_msg("YC")
        # Clear DSD from the main message so flow falls back to datastructure()
        separate_dsd = msg.structure["DSD_YC"]
        msg.structure = {}
        client.dataflow.return_value = msg
        client.datastructure.return_value = SimpleNamespace(structure={"DSD_YC": separate_dsd})
        client.series_keys.return_value = [_sk({"FREQ": "A", "REF_AREA": "U2"})]
        out = list(list_series_flow(client, "ECB", "YC"))
        assert len(out) == 1
        client.datastructure.assert_called_once()

    def test_missing_dataflow_in_response_raises(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = SimpleNamespace(dataflow={}, structure={})
        with pytest.raises(SdmxFetchError, match="missing from response"):
            list(list_series_flow(client, "ECB", "YC"))

    def test_client_exception_on_series_keys_wrapped(self) -> None:
        client = MagicMock()
        client.dataflow.return_value = self._build_msg("YC")
        client.series_keys.side_effect = RuntimeError("timeout")
        with pytest.raises(SdmxFetchError, match="Failed to fetch series keys"):
            list(list_series_flow(client, "ECB", "YC"))

    def test_descendants_param_retry_on_type_error(self) -> None:
        """Agencies that reject the `references=descendants` param fall back."""
        client = MagicMock()
        msg = self._build_msg("YC")

        call_count = 0

        def flaky_dataflow(**kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if "params" in kwargs:
                raise TypeError("no descendants support")
            return msg

        client.dataflow.side_effect = flaky_dataflow
        client.series_keys.return_value = [_sk({"FREQ": "A", "REF_AREA": "U2"})]
        list(list_series_flow(client, "ECB", "YC"))
        assert call_count == 2
