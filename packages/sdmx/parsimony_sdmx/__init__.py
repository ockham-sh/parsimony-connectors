"""``parsimony-sdmx`` — SDMX connector plugin for the ``parsimony`` kernel.

Exports:

- :data:`CONNECTORS` — the plugin surface discovered via the
  ``parsimony.providers`` entry point group. Three items: two enumerators
  (dataset-level + per-dataset series) and one live fetch connector. Both
  enumerators carry a ``catalog=`` declaration so ``parsimony bundles``
  drives publish; SDMX no longer ships its own bundle CLI.
- :data:`ENV_VARS` — empty. SDMX endpoints are public.
- :data:`PROVIDER_METADATA` — bundle topology and supported agencies.

Discovery is driven by the kernel via entry points declared in
``pyproject.toml``. No manual registration required — users ``pip install
parsimony-sdmx`` and the plugin appears in ``parsimony list-plugins``.
"""

from __future__ import annotations

from typing import Any

from parsimony_sdmx.connectors import CONNECTORS, ENV_VARS
from parsimony_sdmx.connectors._agencies import ALL_AGENCIES

__version__ = "0.2.0"


PROVIDER_METADATA: dict[str, Any] = {
    "agencies": [a.value for a in ALL_AGENCIES],
    "namespace_templates": [
        "sdmx_datasets",
        "sdmx_series_{agency}_{dataset_id}",
    ],
    "plugin_version": __version__,
}


__all__ = [
    "CONNECTORS",
    "ENV_VARS",
    "PROVIDER_METADATA",
    "__version__",
]
