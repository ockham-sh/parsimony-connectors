"""Catalog namespace composition — the searchable ``sdmx_{kind}_{agency}[_{id}]`` names.

Pure string composition from an agency (+ dataset id) to the canonical namespace
a search connector reads and the offline build stamps. Two kinds:

- ``sdmx_datasets_<agency>`` — one per-agency dataset catalog.
- ``sdmx_series_<agency>_<flow>`` — one populated-series catalog per flow.

Namespace strings are ``snake_case`` lowercase (the catalog-layer convention);
``AgencyId``/``to_namespace_token`` keep the uppercase SDMX spelling on the outside
and downcase here.
"""

from __future__ import annotations

from parsimony_sdmx.core.agencies import AgencyId, to_namespace_token

#: Prefix for per-agency dataset catalog namespaces.
DATASETS_NAMESPACE_PREFIX = "sdmx_datasets"


def datasets_namespace(agency: AgencyId | str) -> str:
    """Return the canonical per-agency dataset catalog namespace.

    ``AgencyId.ECB`` → ``"sdmx_datasets_ecb"``,
    ``AgencyId.WB_WDI`` → ``"sdmx_datasets_wb_wdi"``.
    """

    return f"{DATASETS_NAMESPACE_PREFIX}_{to_namespace_token(agency)}"


def parse_datasets_namespace(namespace: str) -> AgencyId:
    """Map a dataset catalog namespace back to :class:`AgencyId`."""

    prefix = f"{DATASETS_NAMESPACE_PREFIX}_"
    if not namespace.startswith(prefix):
        raise ValueError(f"Unsupported dataset namespace {namespace!r}")
    token = namespace.removeprefix(prefix)
    for agency in AgencyId:
        if to_namespace_token(agency) == token:
            return agency
    raise ValueError(f"Could not parse agency from dataset namespace {namespace!r}")


def is_datasets_namespace(namespace: str) -> bool:
    """Return whether *namespace* is a per-agency dataset catalog namespace."""

    try:
        parse_datasets_namespace(namespace)
    except ValueError:
        return False
    return True


def series_namespace(agency: AgencyId | str, dataset_id: str) -> str:
    """Return the canonical per-flow series catalog namespace ``sdmx_series_<agency>_<flow>``."""

    return f"sdmx_series_{to_namespace_token(agency)}_{dataset_id.lower()}"


__all__ = [
    "DATASETS_NAMESPACE_PREFIX",
    "datasets_namespace",
    "is_datasets_namespace",
    "parse_datasets_namespace",
    "series_namespace",
]
