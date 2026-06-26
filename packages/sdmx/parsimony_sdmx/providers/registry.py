"""Plain dict mapping agency IDs to provider factories."""

from __future__ import annotations

from collections.abc import Callable

from parsimony_sdmx.providers.ecb import EcbProvider
from parsimony_sdmx.providers.estat import EstatProvider
from parsimony_sdmx.providers.imf import ImfProvider
from parsimony_sdmx.providers.protocol import CatalogProvider
from parsimony_sdmx.providers.wb import WbProvider

ProviderFactory = Callable[[], CatalogProvider]

# Lambdas keep the Protocol return type visible to mypy without a cast.
AGENCIES: dict[str, ProviderFactory] = {
    "ECB": lambda: EcbProvider(),
    "ESTAT": lambda: EstatProvider(),
    "IMF_DATA": lambda: ImfProvider(),
    "WB_WDI": lambda: WbProvider(),
}


def get_provider(agency_id: str) -> CatalogProvider:
    """Resolve an agency ID to a provider instance.

    Raises ``KeyError`` with a helpful message if the agency is unknown.
    """
    factory = AGENCIES.get(agency_id)
    if factory is None:
        known = ", ".join(sorted(AGENCIES)) or "(none)"
        raise KeyError(f"Unknown agency {agency_id!r}. Known: {known}")
    return factory()
