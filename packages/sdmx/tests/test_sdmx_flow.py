from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import DatasetRecord
from parsimony_sdmx.providers.sdmx_flow import (
    list_datasets_flow,
)


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
    return SimpleNamespace(id=dim_id, local_representation=local, concept_identity=None)


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
        client.dataflow.return_value = SimpleNamespace(dataflow={"WDI": _flow("WDI", "World Development Indicators")})
        out = list(list_datasets_flow(client, "WB_WDI", decorate_title=lambda fid, t: f"World Bank - {t}"))
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


class TestListStructureFlow:
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
            name=_named({"en": "Yield curve"}),
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

    def test_structure_record_shape(self) -> None:
        from parsimony_sdmx.providers.sdmx_flow import list_structure_flow

        client = MagicMock()
        client.dataflow.return_value = self._build_msg("YC")
        record = list_structure_flow(client, "ECB", "YC")
        assert record.dataset_id == "YC"
        assert record.dsd_order == ("FREQ", "REF_AREA")
        cl_ids = {cl.codelist_id for cl in record.codelists}
        assert "CL_FREQ" in cl_ids
        assert "CL_AREA" in cl_ids
        assert record.dimensions[0].sample[0].code == "A"
