"""Agency allowlist for connector-side routing and Pydantic validation.

Separate from :mod:`parsimony_sdmx.providers.registry` (which is the
write-side catalog-builder registry). This module is the SSRF boundary
for live ``sdmx_fetch`` and the input allowlist for both enumerators.

Convention for the catalog-layer kernel (enforced by ``normalize_code``):
namespace strings are ``snake_case`` lowercase. This module keeps the
uppercase SDMX convention on the outside (``"ECB"``) and exposes a
:func:`to_namespace_token` helper that downcases for namespace
composition.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final


class AgencyId(StrEnum):
    """SDMX source identifiers supported by this plugin.

    Values match the ``sdmx1`` library's built-in source registry so we
    can pass the enum value straight to ``sdmx_lib.Client(agency.value)``.
    """

    ECB = "ECB"
    ESTAT = "ESTAT"
    IMF_DATA = "IMF_DATA"
    WB_WDI = "WB_WDI"


#: Ordered list of all supported agencies — iterating `AgencyId` preserves
#: declaration order.
ALL_AGENCIES: Final[tuple[AgencyId, ...]] = tuple(AgencyId)


def to_namespace_token(agency: AgencyId | str) -> str:
    """Convert an agency ID to its lowercase namespace-safe token.

    ``AgencyId.ECB`` → ``"ecb"``, ``AgencyId.IMF_DATA`` → ``"imf_data"``.
    """
    raw = agency.value if isinstance(agency, AgencyId) else str(agency)
    return raw.lower()
