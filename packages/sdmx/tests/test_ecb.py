"""Tests for the ECB provider glue: dsd_order → attrs fetch → augment → flow."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from parsimony_sdmx.core.errors import SdmxFetchError
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.providers.ecb import EcbProvider, _build_augment, _fetch_series_attributes
from parsimony_sdmx.providers.ecb_series_attrs import GENERIC_NS

SERIES_ATTRS_XML = (
    f'<message xmlns:generic="{GENERIC_NS}">'
    f'<generic:Series>'
    f'<generic:SeriesKey>'
    f'<generic:Value id="FREQ" value="A"/>'
    f'<generic:Value id="REF_AREA" value="U2"/>'
    f'</generic:SeriesKey>'
    f'<generic:Attributes>'
    f'<generic:Value id="TITLE" value="Annual EU"/>'
    f'<generic:Value id="TITLE_COMPL" value="Annual growth rate, Euro area"/>'
    f'</generic:Attributes>'
    f'</generic:Series>'
    f'</message>'
).encode()


class TestBuildAugment:
    def test_applies_title_and_compl_from_map(self) -> None:
        attrs_map: dict[str, tuple[str | None, str | None]] = {
            "A.U2": ("Annual EU", "Annual growth rate, Euro area"),
        }
        augment = _build_augment(attrs_map)
        result = augment("FREQ: Annual - REF_AREA: Euro area", "A.U2")
        assert "Annual EU" in result
        assert "Annual growth rate" in result

    def test_unknown_series_returns_base(self) -> None:
        augment = _build_augment({})
        # Missing series → base unchanged (no augmentation suffix).
        result = augment("base title", "not.in.map")
        assert result == "base title"


class TestFetchSeriesAttributes:
    def test_fetch_failure_returns_empty_map(self) -> None:
        # A failed ECB attrs fetch must not abort the run — the provider
        # falls back to un-augmented titles.
        from parsimony_sdmx.io.http import HttpConfig

        with patch(
            "parsimony_sdmx.providers.ecb.bounded_get",
            side_effect=SdmxFetchError("upstream 503"),
        ):
            attrs = _fetch_series_attributes("YC", ["FREQ", "REF_AREA"], HttpConfig())
        assert attrs == {}

    def test_fetch_success_parses_attrs(self) -> None:
        from parsimony_sdmx.io.http import HttpConfig

        with patch(
            "parsimony_sdmx.providers.ecb.bounded_get",
            return_value=SERIES_ATTRS_XML,
        ):
            attrs = _fetch_series_attributes(
                "YC", ["FREQ", "REF_AREA"], HttpConfig()
            )
        assert attrs == {
            "A.U2": ("Annual EU", "Annual growth rate, Euro area"),
        }

    def test_parse_failure_returns_empty_map(self) -> None:
        from parsimony_sdmx.io.http import HttpConfig

        with patch(
            "parsimony_sdmx.providers.ecb.bounded_get",
            return_value=b"<not><closed>",
        ):
            attrs = _fetch_series_attributes(
                "YC", ["FREQ", "REF_AREA"], HttpConfig()
            )
        assert attrs == {}

    def test_http_config_read_timeout_halved(self) -> None:
        # The attrs fetch uses a tighter read_timeout so a hung upstream
        # doesn't hold the subprocess for the full dataset timeout.
        from parsimony_sdmx.io.http import HttpConfig

        captured: list[HttpConfig] = []

        def _fake_get(
            session: object, url: str, config: HttpConfig, extra_headers: dict[str, str]
        ) -> bytes:
            captured.append(config)
            return SERIES_ATTRS_XML

        base = HttpConfig(read_timeout=300.0)
        with patch("parsimony_sdmx.providers.ecb.bounded_get", side_effect=_fake_get):
            _fetch_series_attributes("YC", ["FREQ", "REF_AREA"], base)
        assert captured[0].read_timeout == 150.0
        # Other fields are preserved by dataclasses.replace.
        assert captured[0].connect_timeout == base.connect_timeout
        assert captured[0].max_retries == base.max_retries

    def test_read_timeout_floor(self) -> None:
        from parsimony_sdmx.io.http import HttpConfig

        captured: list[HttpConfig] = []

        def _fake_get(
            session: object, url: str, config: HttpConfig, extra_headers: dict[str, str]
        ) -> bytes:
            captured.append(config)
            return SERIES_ATTRS_XML

        tiny = HttpConfig(read_timeout=10.0)  # / 2 = 5, but floor is 30
        with patch("parsimony_sdmx.providers.ecb.bounded_get", side_effect=_fake_get):
            _fetch_series_attributes("YC", ["FREQ", "REF_AREA"], tiny)
        assert captured[0].read_timeout == 30.0


class TestEcbProviderListSeries:
    """End-to-end happy path for EcbProvider.list_series with everything mocked."""

    def _build_msg(self) -> MagicMock:
        msg = MagicMock()
        dataflow = MagicMock()
        dataflow.structure = MagicMock()
        dataflow.structure.id = "DSD_YC"
        msg.dataflow = {"YC": dataflow}
        # DSD whose dimensions iterate in order → dim_order = [FREQ, REF_AREA].
        # TIME_PERIOD is excluded by extract_dsd_dim_order.
        dims = [
            _stub_dim("FREQ"),
            _stub_dim("REF_AREA"),
            _stub_dim("TIME_PERIOD"),
        ]
        dsd = MagicMock()
        dsd.dimensions = dims  # direct iteration yields each dim
        # codelist map empty → labels dict empty → titles fall back to raw codes
        msg.structure = {"DSD_YC": dsd}
        msg.codelist = {}
        return msg

    def _client(self, msg: MagicMock) -> MagicMock:
        client = MagicMock()
        client.dataflow.return_value = msg
        return client

    def test_emits_augmented_series_records(self) -> None:
        msg = self._build_msg()
        client = self._client(msg)

        series_key = MagicMock()
        series_key.values = [
            _stub_value("FREQ", "A"),
            _stub_value("REF_AREA", "U2"),
        ]
        client.series_keys.return_value = [series_key]

        @contextmanager
        def _fake_sdmx_client(agency: str, http_config: object) -> Iterator[MagicMock]:
            yield client

        with (
            patch("parsimony_sdmx.providers.ecb.sdmx_client", _fake_sdmx_client),
            patch(
                "parsimony_sdmx.providers.ecb.bounded_get",
                return_value=SERIES_ATTRS_XML,
            ),
            patch(
                "parsimony_sdmx.providers.sdmx_flow.fetch_dataflow_with_structure",
                return_value=msg,
            ),
            patch(
                "parsimony_sdmx.providers.sdmx_flow.resolve_dsd",
                return_value=msg.structure["DSD_YC"],
            ),
        ):
            provider = EcbProvider(agency_id="ECB")
            records = list(provider.list_series("YC"))

        assert len(records) == 1
        rec = records[0]
        assert isinstance(rec, SeriesRecord)
        assert rec.dataset_id == "YC"
        # Augmented title contains the fetched TITLE/TITLE_COMPL strings.
        assert "Annual EU" in rec.title
        assert "Annual growth rate" in rec.title

    def test_falls_back_when_attrs_fetch_fails(self) -> None:
        msg = self._build_msg()
        client = self._client(msg)

        series_key = MagicMock()
        series_key.values = [
            _stub_value("FREQ", "A"),
            _stub_value("REF_AREA", "U2"),
        ]
        client.series_keys.return_value = [series_key]

        @contextmanager
        def _fake_sdmx_client(agency: str, http_config: object) -> Iterator[MagicMock]:
            yield client

        with (
            patch("parsimony_sdmx.providers.ecb.sdmx_client", _fake_sdmx_client),
            patch(
                "parsimony_sdmx.providers.ecb.bounded_get",
                side_effect=SdmxFetchError("upstream 503"),
            ),
            patch(
                "parsimony_sdmx.providers.sdmx_flow.fetch_dataflow_with_structure",
                return_value=msg,
            ),
            patch(
                "parsimony_sdmx.providers.sdmx_flow.resolve_dsd",
                return_value=msg.structure["DSD_YC"],
            ),
        ):
            provider = EcbProvider(agency_id="ECB")
            records = list(provider.list_series("YC"))

        assert len(records) == 1
        # Un-augmented: only the base codelist title, no TITLE/TITLE_COMPL suffix.
        assert "Annual EU" not in records[0].title
        assert "Annual growth rate" not in records[0].title

    def test_missing_dataflow_raises_sdmx_fetch_error(self) -> None:
        msg = MagicMock()
        msg.dataflow = {}  # YC missing
        client = self._client(msg)

        @contextmanager
        def _fake_sdmx_client(agency: str, http_config: object) -> Iterator[MagicMock]:
            yield client

        with (
            patch("parsimony_sdmx.providers.ecb.sdmx_client", _fake_sdmx_client),
            patch(
                "parsimony_sdmx.providers.sdmx_flow.fetch_dataflow_with_structure",
                return_value=msg,
            ),
        ):
            provider = EcbProvider(agency_id="ECB")
            with pytest.raises(SdmxFetchError, match="missing"):
                list(provider.list_series("YC"))


def _stub_dim(dim_id: str) -> MagicMock:
    d = MagicMock()
    d.id = dim_id
    # Only non-TIME_PERIOD dims need a concept+local_representation to resolve codes.
    d.local_representation = MagicMock()
    d.local_representation.enumerated = MagicMock()
    d.local_representation.enumerated.id = f"CL_{dim_id}"
    return d


def _stub_value(dim_id: str, code: str) -> MagicMock:
    v = MagicMock()
    v.id = dim_id  # extract_series_dim_values reads kv.id when iterating
    v.value = code
    return v
