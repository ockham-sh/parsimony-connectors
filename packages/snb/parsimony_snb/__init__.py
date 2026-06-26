"""Swiss National Bank (SNB) connector — fetch + catalog enumeration.

Data portal: https://data.snb.ch. No authentication required (keyless public
CSV/JSON API — no ``secrets=``/``bind()``/``UnauthorizedError``; ``load()`` binds
only the catalog URL for search).

Discovery (archetype A — see :mod:`parsimony_snb.connectors.enumerate`):

The authoritative universe is the published XML **sitemap** (``/sitemap``), which
lists every cube URL — **1,149** cubes in two families:

* **Publication cubes** (237): ``/topics/{topic}/cube/{id}``, bare ids
  (``rendoblim``), fetched via ``/api/cube/{id}/data/csv/{lang}``. Catalogued at
  series granularity (compound ``cube_id#series_key`` codes).
* **Warehouse cubes** (912): ``/warehouse/{group}/cube/{sdmx_id}``, SDMX ids
  (``BSTA@SNB.AUR_U.ODF``), fetched via ``/api/warehouse/cube/{id}/data/csv/{lang}``
  with the id's ``@`` mapped to ``.``. Catalogued at cube granularity.

This replaces the prior frozen ``_KNOWN_CUBES`` registry (the guidebook's named
cautionary case) with a self-tracking sitemap crawl, and adds the data warehouse
that the old connector excluded.

Transport notes: the public ``/api/...`` data paths are plain ``httpx``. Cube
titles come from the portal-internal ``/json/table/getCubeInfo`` endpoint, which
is WAF-walled unless an ``x-epb-ajax: true`` header is sent — a best-effort
enrichment (the completeness surface never depends on it). See
:mod:`parsimony_snb._http`.

This module is a thin facade: the implementation lives in :mod:`parsimony_snb._http`,
:mod:`parsimony_snb.parsing`, :mod:`parsimony_snb.outputs`,
:mod:`parsimony_snb.connectors`, and :mod:`parsimony_snb.search`.
"""

from __future__ import annotations

from parsimony_snb.connectors import CONNECTORS, load
from parsimony_snb.connectors.enumerate import enumerate_snb
from parsimony_snb.connectors.fetch import snb_fetch
from parsimony_snb.outputs import SNB_ENUMERATE_OUTPUT, SNB_FETCH_OUTPUT
from parsimony_snb.search import snb_search

__all__ = ["CONNECTORS", "load"]

# Re-exported for downstream/test convenience; the discovered plugin surface is
# CONNECTORS and the only public entry points are CONNECTORS + load (hence __all__).
_ = (enumerate_snb, snb_fetch, snb_search, SNB_ENUMERATE_OUTPUT, SNB_FETCH_OUTPUT)
