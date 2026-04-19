from unittest.mock import MagicMock, patch

import pytest
import requests

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.io.http import HttpConfig
from parsimony_sdmx.providers.wb import (
    GENERIC_NS,
    WB_SDMX_AGENCY,
    WbConfig,
    WbProvider,
    _build_decades,
    _build_path_combinations,
    _codes_per_dim,
    _fetch_path_decade,
    _fetch_wb_structure,
    _NoFetchClient,
    _parse_series_ids,
    _split_to_dict,
    discover_wb_series,
)


def _xml_series(key_values: dict[str, str]) -> str:
    kvs = "".join(
        f'<generic:Value id="{k}" value="{v}"/>' for k, v in key_values.items()
    )
    return (
        f"<generic:Series>"
        f"<generic:SeriesKey>{kvs}</generic:SeriesKey>"
        f"</generic:Series>"
    )


def _xml(series: list[dict[str, str]]) -> bytes:
    body = "".join(_xml_series(s) for s in series)
    return f'<doc xmlns:generic="{GENERIC_NS}">{body}</doc>'.encode()


class TestBuildDecades:
    def test_default_decades(self) -> None:
        decades = _build_decades(WbConfig())
        assert decades[0] == ("1950", "1959")
        assert decades[-1] == ("2020", "2029")
        assert len(decades) == 8

    def test_custom_range(self) -> None:
        cfg = WbConfig(decade_start=2000, decade_end=2020, decade_step=10)
        assert _build_decades(cfg) == [("2000", "2009"), ("2010", "2019")]


class TestBuildPathCombinations:
    def test_cartesian_over_first_n_dims(self) -> None:
        dim_order = ("FREQ", "REF_AREA", "INDICATOR")
        dim_codes = {
            "FREQ": ["A", "M"],
            "REF_AREA": ["U2", "US"],
            "INDICATOR": ["X", "Y", "Z"],
        }
        paths = _build_path_combinations(dim_order, dim_codes, max_base_dims=2)
        # 2 FREQ × 2 REF_AREA × 1 wildcard = 4 paths
        assert set(paths) == {"A.U2.", "A.US.", "M.U2.", "M.US."}

    def test_empty_base_codes_returns_full_wildcard(self) -> None:
        # N dims → N empty strings joined by "." → (N-1) dots.
        paths2 = _build_path_combinations(
            ("FREQ", "REF_AREA"),
            {"FREQ": [], "REF_AREA": ["U2"]},
            max_base_dims=2,
        )
        assert paths2 == ["."]
        paths3 = _build_path_combinations(
            ("A", "B", "C"),
            {"A": [], "B": ["x"], "C": ["y"]},
            max_base_dims=2,
        )
        assert paths3 == [".."]

    def test_empty_dim_order_yields_nothing(self) -> None:
        assert _build_path_combinations((), {}, 2) == []

    def test_max_base_dims_caps_cartesian_size(self) -> None:
        dim_order = ("A", "B", "C", "D")
        dim_codes = {"A": ["1", "2"], "B": ["x"], "C": ["p", "q"], "D": ["z"]}
        paths = _build_path_combinations(dim_order, dim_codes, max_base_dims=1)
        assert set(paths) == {"1...", "2..."}


class TestCodesPerDim:
    def test_extracts_code_ids(self) -> None:
        raw = {
            "FREQ": {"A": {"en": "Annual"}, "M": {"en": "Monthly"}},
            "REF_AREA": {"U2": {"en": "Euro"}},
        }
        out = _codes_per_dim(raw, ("FREQ", "REF_AREA", "MISSING"))
        assert set(out["FREQ"]) == {"A", "M"}
        assert out["REF_AREA"] == ["U2"]
        assert out["MISSING"] == []


class TestSplitToDict:
    def test_happy_path(self) -> None:
        assert _split_to_dict("A.U2.EUR", ("FREQ", "REF_AREA", "CURRENCY")) == {
            "FREQ": "A",
            "REF_AREA": "U2",
            "CURRENCY": "EUR",
        }

    def test_wrong_part_count_returns_empty(self) -> None:
        assert _split_to_dict("A.U2", ("FREQ", "REF_AREA", "CURRENCY")) == {}


class TestParseSeriesIds:
    def test_extracts_dotted_ids(self) -> None:
        xml = _xml([{"FREQ": "A", "REF_AREA": "U2"}, {"FREQ": "M", "REF_AREA": "US"}])
        out = _parse_series_ids(xml, ("FREQ", "REF_AREA"))
        assert out == {"A.U2", "M.US"}

    def test_skips_malformed_series(self) -> None:
        xml = _xml([{"FREQ": "A"}])  # missing REF_AREA
        out = _parse_series_ids(xml, ("FREQ", "REF_AREA"))
        assert out == set()

    def test_malformed_xml_returns_empty(self) -> None:
        out = _parse_series_ids(b"<not><closed>", ("FREQ",))
        assert out == set()


class TestFetchPathDecade:
    def _mock_response(
        self, status: int = 200, body: bytes = b""
    ) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = status
        resp.url = "https://x/"
        # Body is delivered in one chunk to iter_content (matches the
        # bounded-read loop in _fetch_path_decade).
        resp.iter_content.return_value = iter([body]) if body else iter([])
        if status >= 400 and status != 404:
            resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    def test_404_returns_empty_no_raise(self) -> None:
        session = MagicMock()
        session.get.return_value = self._mock_response(status=404)
        out = _fetch_path_decade(session, "https://x/", HttpConfig(), ("FREQ",))
        assert out == set()

    def test_200_parses_body(self) -> None:
        session = MagicMock()
        body = _xml([{"FREQ": "A"}])
        session.get.return_value = self._mock_response(status=200, body=body)
        out = _fetch_path_decade(session, "https://x/", HttpConfig(), ("FREQ",))
        assert out == {"A"}

    def test_5xx_raises_sdmx_fetch_error(self) -> None:
        # Review finding #11: non-404 non-2xx must fail the dataset, not
        # silently return empty (which hides catalog outages).
        session = MagicMock()
        session.get.return_value = self._mock_response(status=503)
        with pytest.raises(SdmxFetchError, match="HTTP 503"):
            _fetch_path_decade(session, "https://x/", HttpConfig(), ("FREQ",))

    def test_4xx_non_404_raises_sdmx_fetch_error(self) -> None:
        session = MagicMock()
        session.get.return_value = self._mock_response(status=403)
        with pytest.raises(SdmxFetchError, match="HTTP 403"):
            _fetch_path_decade(session, "https://x/", HttpConfig(), ("FREQ",))

    def test_connection_error_raises_sdmx_fetch_error(self) -> None:
        # Review finding #11: network errors must fail the dataset so
        # full outages show up as FAILED, not as "successfully empty".
        session = MagicMock()
        session.get.side_effect = requests.exceptions.ConnectionError("nope")
        with pytest.raises(SdmxFetchError, match="network error"):
            _fetch_path_decade(session, "https://x/", HttpConfig(), ("FREQ",))

    def test_body_over_cap_raises(self) -> None:
        session = MagicMock()
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.url = "https://x/"
        huge = b"x" * 100_000
        resp.iter_content.return_value = iter([huge])
        resp.raise_for_status.return_value = None
        session.get.return_value = resp
        tiny = HttpConfig(max_response_bytes=1024)
        with pytest.raises(SdmxFetchError, match="exceeded"):
            _fetch_path_decade(session, "https://x/", tiny, ("FREQ",))


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
            msg = _fetch_wb_structure(
                HttpConfig(), "https://api.worldbank.org/v2/sdmx/rest", "dataflow/WB"
            )
        assert list(msg.dataflow.keys()) == ["WDI"]

    def test_http_error_wrapped_as_sdmx_fetch_error(self) -> None:
        with patch(
            "parsimony_sdmx.providers.wb.bounded_get",
            side_effect=SdmxFetchError("403 Forbidden"),
        ):
            with pytest.raises(SdmxFetchError, match="WB structure fetch"):
                _fetch_wb_structure(
                    HttpConfig(),
                    "https://api.worldbank.org/v2/sdmx/rest",
                    "dataflow/WB",
                )

    def test_parse_error_wrapped_as_sdmx_fetch_error(self) -> None:
        with patch(
            "parsimony_sdmx.providers.wb.bounded_get",
            return_value=b"<not-sdmx><garbage>",
        ):
            with pytest.raises(SdmxFetchError, match="parse"):
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
        with patch(
            "parsimony_sdmx.providers.wb.bounded_get", return_value=xml
        ) as mock_get:
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


class TestDiscoverWbSeries:
    def test_aggregates_across_decades(self) -> None:
        cfg = WbConfig(decade_start=2000, decade_end=2020, decade_step=10, max_workers=1)

        def fake_get(url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock(spec=requests.Response)
            resp.status_code = 200
            resp.url = url
            if "startPeriod=2000" in url:
                resp.iter_content.return_value = iter([_xml([{"FREQ": "A"}])])
            else:
                resp.iter_content.return_value = iter([_xml([{"FREQ": "M"}])])
            resp.raise_for_status.return_value = None
            return resp

        session = MagicMock()
        session.get.side_effect = fake_get
        out = discover_wb_series(
            session=session,
            base_url="https://x",
            dataset_id="WDI",
            dim_order=("FREQ",),
            dim_codes={"FREQ": ["A", "M"]},
            http_config=HttpConfig(),
            wb_config=cfg,
        )
        assert out == {"A", "M"}

    def test_all_404s_returns_empty(self) -> None:
        cfg = WbConfig(decade_start=2000, decade_end=2010, decade_step=10, max_workers=1)

        def fake_get(url: str, **kwargs: object) -> MagicMock:
            resp = MagicMock(spec=requests.Response)
            resp.status_code = 404
            return resp

        session = MagicMock()
        session.get.side_effect = fake_get
        out = discover_wb_series(
            session=session,
            base_url="https://x",
            dataset_id="WDI",
            dim_order=("FREQ",),
            dim_codes={"FREQ": ["A"]},
            http_config=HttpConfig(),
            wb_config=cfg,
        )
        assert out == set()
