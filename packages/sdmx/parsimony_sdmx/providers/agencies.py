"""Lightweight agency-ID registry — no provider class imports.

Kept separate from :mod:`parsimony_sdmx.providers.registry` so the parent
CLI process can enumerate agency names (for argparse choices, help text,
error messages) without triggering the transitive ``sdmx1`` import
chain that ``registry`` → provider modules → ``sdmx_client`` pull in.

Keeping the parent sdmx1-free matters because ``sdmx1`` maintains a
module-level cache with no public invalidation hook; any process that
imports it pays cache-retention cost for its entire lifetime.
"""

from __future__ import annotations

AGENCY_IDS: tuple[str, ...] = ("ECB", "ESTAT", "IMF_DATA", "WB_WDI")
