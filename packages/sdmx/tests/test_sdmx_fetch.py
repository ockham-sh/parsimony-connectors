"""Tests for ``parsimony_sdmx.connectors.fetch.sdmx_fetch``.

The connector reaches out to the live SDMX endpoint via ``sdmx1.Client``.
We replace ``sdmx_client`` with a context manager yielding a fake client,
and stub ``sdmx.to_pandas`` so the entire test runs without hitting the
network and without depending on real SDMX message shapes.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError


def _fake_dsd(dim_ids: list[str]) -> Any:
    """Fake DSD whose ``dimensions`` iterable yields objects with an ``id``."""
    return SimpleNamespace(
        dimensions=[SimpleNamespace(id=d) for d in dim_ids],
    )


def _make_fake_client(
    *,
    structure_msg: Any,
    data_msg: Any,
    raise_on_get: Exception | None = None,
) -> MagicMock:
    client = MagicMock()
    client.dataflow.return_value = structure_msg
    client.datastructure.return_value = structure_msg
    if raise_on_get is not None:
        client.get.side_effect = raise_on_get
    else:
        client.get.return_value = data_msg
    return client


def _structure_msg(dim_ids: list[str], dataset_id: str) -> Any:
    dsd = _fake_dsd(dim_ids + ["TIME_PERIOD"])
    dataflow = SimpleNamespace(structure=SimpleNamespace(id="DSD_X"))
    return SimpleNamespace(
        dataflow={dataset_id: dataflow},
        structure={"DSD_X": dsd},
        codelist={},
    )


@pytest.fixture
def patch_sdmx(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace ``sdmx_client`` and ``sdmx.to_pandas`` with controllable stubs."""
    state: dict[str, Any] = {"client": None, "to_pandas_input": None}

    @contextmanager
    def _fake_sdmx_client(_agency_id: str, **_kwargs: Any) -> Any:
        yield state["client"]

    monkeypatch.setattr(
        "parsimony_sdmx.providers.sdmx_client.sdmx_client",
        _fake_sdmx_client,
    )

    def _fake_to_pandas(data: Any) -> pd.DataFrame:
        state["to_pandas_input"] = data
        return state["to_pandas_output"]

    import sdmx as sdmx_lib

    monkeypatch.setattr(sdmx_lib, "to_pandas", _fake_to_pandas)
    return state


class TestSdmxFetch:
    def test_returns_table_result_with_expected_schema(
        self, patch_sdmx: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams, sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA", "CURRENCY"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(
            structure_msg=structure_msg,
            data_msg=data_msg,
        )
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {
                "FREQ": ["M", "M"],
                "REF_AREA": ["DE", "FR"],
                "CURRENCY": ["EUR", "EUR"],
                "TIME_PERIOD": ["2024-01", "2024-01"],
                "value": [1.0, 2.0],
            }
        ).set_index(["FREQ", "REF_AREA", "CURRENCY", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(
            dataset_key="ECB-YC",
            series_key="M.DE.EUR",
        )
        result = asyncio.run(sdmx_fetch(params))

        df = result.data
        assert list(df.columns) == [
            "series_key",
            "title",
            "FREQ",
            "REF_AREA",
            "CURRENCY",
            "TIME_PERIOD",
            "value",
        ]
        assert list(df["series_key"]) == ["M.DE.EUR", "M.FR.EUR"]
        assert list(df["FREQ"]) == ["M", "M"]
        # Title falls back to series_key when no labels are known.
        assert all(df["title"].str.len() > 0)

    def test_empty_data_raises_empty_data_error(
        self, patch_sdmx: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams, sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(
            structure_msg=structure_msg, data_msg=data_msg
        )
        patch_sdmx["to_pandas_output"] = pd.Series([], name="value", dtype=float)

        params = SdmxFetchParams(dataset_key="ECB-YC", series_key="M.DE")
        with pytest.raises(EmptyDataError):
            asyncio.run(sdmx_fetch(params))

    def test_unknown_agency_rejected_at_param_validation(self) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        with pytest.raises(ValueError, match="Unknown agency"):
            SdmxFetchParams(dataset_key="BOGUS-X", series_key="M.DE")

    def test_dataset_key_must_include_agency_prefix(self) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams

        with pytest.raises(ValueError, match="must include agency prefix"):
            SdmxFetchParams(dataset_key="YCONLY", series_key="M.DE")

    def test_series_url_metadata_for_ecb(
        self, patch_sdmx: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams, sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(
            structure_msg=structure_msg, data_msg=data_msg
        )
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {
                "FREQ": ["M"],
                "REF_AREA": ["DE"],
                "TIME_PERIOD": ["2024-01"],
                "value": [1.0],
            }
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(dataset_key="ECB-YC", series_key="M.DE")
        result = asyncio.run(sdmx_fetch(params))

        metadata = result.provenance.properties.get("metadata", [])
        urls = [m["value"] for m in metadata if m.get("name") == "series_url"]
        assert urls, "expected series_url metadata entry"
        parsed = urlparse(urls[0])
        assert parsed.scheme == "https"
        assert parsed.hostname == "data.ecb.europa.eu"

    def test_no_series_url_for_wb_wdi(
        self, patch_sdmx: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.connectors.fetch import SdmxFetchParams, sdmx_fetch

        dim_ids = ["FREQ", "REF_AREA"]
        structure_msg = _structure_msg(dim_ids, "WDI")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(
            structure_msg=structure_msg, data_msg=data_msg
        )
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {
                "FREQ": ["A"],
                "REF_AREA": ["USA"],
                "TIME_PERIOD": ["2024"],
                "value": [1.0],
            }
        ).set_index(["FREQ", "REF_AREA", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(dataset_key="WB_WDI-WDI", series_key="A.USA")
        result = asyncio.run(sdmx_fetch(params))
        assert result.provenance.properties.get("metadata", []) == []

    def test_namespace_uses_normalized_dataset_key(
        self, patch_sdmx: dict[str, Any]
    ) -> None:
        from parsimony_sdmx.connectors.fetch import (
            SdmxFetchParams,
            _sdmx_namespace_from_dataset_key,
            sdmx_fetch,
        )

        ns = _sdmx_namespace_from_dataset_key("ECB-YC")
        assert ns.startswith("sdmx_")

        # Round-trip through the actual fetch to confirm the namespace shows up.
        dim_ids = ["FREQ"]
        structure_msg = _structure_msg(dim_ids, "YC")
        data_msg = SimpleNamespace(data="<observations>")
        patch_sdmx["client"] = _make_fake_client(
            structure_msg=structure_msg, data_msg=data_msg
        )
        patch_sdmx["to_pandas_output"] = pd.DataFrame(
            {"FREQ": ["M"], "TIME_PERIOD": ["2024-01"], "value": [1.0]}
        ).set_index(["FREQ", "TIME_PERIOD"])["value"]

        params = SdmxFetchParams(dataset_key="ECB-YC", series_key="M")
        result = asyncio.run(sdmx_fetch(params))
        # The series_key column carries the namespace via OutputConfig.
        assert "series_key" in result.data.columns


class TestSdmxFetchOutputBuilder:
    def test_columns_in_expected_order_and_roles(self) -> None:
        from parsimony.result import ColumnRole

        from parsimony_sdmx.connectors.fetch import _sdmx_fetch_output

        out = _sdmx_fetch_output("sdmx_test", ["FREQ", "REF_AREA"])
        names = [c.name for c in out.columns]
        assert names == ["series_key", "title", "FREQ", "REF_AREA", "TIME_PERIOD", "value"]

        roles = {c.name: c.role for c in out.columns}
        assert roles["series_key"] == ColumnRole.KEY
        assert roles["title"] == ColumnRole.TITLE
        assert roles["FREQ"] == ColumnRole.METADATA
        assert roles["REF_AREA"] == ColumnRole.METADATA
        assert roles["TIME_PERIOD"] == ColumnRole.DATA
        assert roles["value"] == ColumnRole.DATA
