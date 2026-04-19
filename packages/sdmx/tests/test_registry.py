import pytest

from parsimony_sdmx.providers.ecb import EcbProvider
from parsimony_sdmx.providers.estat import EstatProvider
from parsimony_sdmx.providers.imf import ImfProvider
from parsimony_sdmx.providers.protocol import CatalogProvider
from parsimony_sdmx.providers.registry import AGENCIES, get_provider
from parsimony_sdmx.providers.wb import WbProvider


class TestRegistry:
    def test_ecb_registered(self) -> None:
        p = get_provider("ECB")
        assert isinstance(p, EcbProvider)
        assert p.agency_id == "ECB"

    def test_estat_registered(self) -> None:
        p = get_provider("ESTAT")
        assert isinstance(p, EstatProvider)
        assert p.agency_id == "ESTAT"

    def test_imf_registered(self) -> None:
        p = get_provider("IMF_DATA")
        assert isinstance(p, ImfProvider)
        assert p.agency_id == "IMF_DATA"

    def test_wb_registered(self) -> None:
        p = get_provider("WB_WDI")
        assert isinstance(p, WbProvider)
        assert p.agency_id == "WB_WDI"

    def test_providers_satisfy_protocol(self) -> None:
        assert isinstance(EcbProvider(), CatalogProvider)
        assert isinstance(EstatProvider(), CatalogProvider)
        assert isinstance(ImfProvider(), CatalogProvider)
        assert isinstance(WbProvider(), CatalogProvider)

    def test_unknown_agency_raises(self) -> None:
        with pytest.raises(KeyError, match="Unknown agency"):
            get_provider("NOPE")

    def test_registry_is_plain_dict(self) -> None:
        assert isinstance(AGENCIES, dict)
        assert set(AGENCIES) == {"ECB", "ESTAT", "IMF_DATA", "WB_WDI"}
