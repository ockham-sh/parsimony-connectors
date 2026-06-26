"""US Energy Information Administration (EIA): fetch + catalog enumeration.

API docs: https://www.eia.gov/opendata/documentation.php

The EIA v2 Open Data API is keyed. Supply ``EIA_API_KEY`` (or bind it via
``load(api_key=...)`` / ``Connector.bind``); a missing key fast-fails with
:class:`~parsimony.errors.UnauthorizedError`. Register a free key at
https://www.eia.gov/opendata/register.php.
"""

from __future__ import annotations

from parsimony_eia.connectors import CONNECTORS, load

__all__ = ["CONNECTORS", "load"]
