from unittest.mock import patch

import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.http import HttpConfig
from parsimony_sdmx.providers.wb import (
    WB_SDMX_AGENCY,
    WbProvider,
    _fetch_wb_structure,
    _NoFetchClient,
)


class TestFetchWbStructure:
    """Structure fetch must bypass sdmx1's ``/latest`` URL pattern.

    WB's gateway 307-redirects ``/latest`` to a deprecated HTTP host
    that returns 403. Our fix fetches raw bytes directly from the
    working URL and parses with ``sdmx.read_sdmx``.
    """

    _MIN_DATAFLOW_XML = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<Structure xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message">'
        b"<Header><ID>x</ID><Test>false</Test><Prepared>2026-01-01T00:00:00</Prepared>"
        b'<Sender id="u"/><Receiver id="u"/></Header>'
        b"<Structures>"
        b'<Dataflows xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">'
        b'<Dataflow id="WDI" urn="urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=WB:WDI(1.0)"'
        b' agencyID="WB" version="1.0" isFinal="true">'
        b'<Name xml:lang="en" xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        b"World Development Indicators</Name>"
        b"<Structure>"
        b'<Ref id="WDI" version="1.0" agencyID="WB" package="datastructure"'
        b' class="DataStructure" xmlns=""/>'
        b"</Structure>"
        b"</Dataflow>"
        b"</Dataflows>"
        b"</Structures>"
        b"</Structure>"
    )

    def test_returns_sdmx_message_with_dataflows(self) -> None:
        with patch(
            "parsimony_sdmx.providers.wb.bounded_get",
            return_value=self._MIN_DATAFLOW_XML,
        ):
            msg = _fetch_wb_structure(HttpConfig(), "https://api.worldbank.org/v2/sdmx/rest", "dataflow/WB")
        assert list(msg.dataflow.keys()) == ["WDI"]

    def test_http_error_wrapped_as_sdmx_fetch_error(self) -> None:
        with (
            patch(
                "parsimony_sdmx.providers.wb.bounded_get",
                side_effect=SdmxFetchError("403 Forbidden"),
            ),
            pytest.raises(SdmxFetchError, match="WB structure fetch"),
        ):
            _fetch_wb_structure(
                HttpConfig(),
                "https://api.worldbank.org/v2/sdmx/rest",
                "dataflow/WB",
            )

    def test_parse_error_wrapped_as_sdmx_fetch_error(self) -> None:
        with (
            patch(
                "parsimony_sdmx.providers.wb.bounded_get",
                return_value=b"<not-sdmx><garbage>",
            ),
            pytest.raises(SdmxFetchError, match="parse"),
        ):
            _fetch_wb_structure(
                HttpConfig(),
                "https://api.worldbank.org/v2/sdmx/rest",
                "dataflow/WB",
            )


class TestNoFetchClient:
    def test_datastructure_raises(self) -> None:
        client = _NoFetchClient()
        with pytest.raises(SdmxFetchError, match="DSD lookup required"):
            client.datastructure(resource_id="WDI")


class TestWbProviderListDatasets:
    """Integration: ``list_datasets`` must go through the direct fetch,
    not through ``sdmx_client``. We assert the URL shape reaches the
    HTTP layer without ``/latest``."""

    def test_calls_dataflow_endpoint_without_latest(self) -> None:
        xml = TestFetchWbStructure._MIN_DATAFLOW_XML
        with patch("parsimony_sdmx.providers.wb.bounded_get", return_value=xml) as mock_get:
            records = list(WbProvider().list_datasets())
        mock_get.assert_called_once()
        url = mock_get.call_args.args[1]
        assert url == f"https://api.worldbank.org/v2/sdmx/rest/dataflow/{WB_SDMX_AGENCY}"
        assert "/latest" not in url
        assert records == [
            type(records[0])(
                dataset_id="WDI",
                agency_id="WB_WDI",
                title="World Bank - World Development Indicators",
            )
        ]
