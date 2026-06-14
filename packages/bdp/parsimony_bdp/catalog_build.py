"""Build the Banco de Portugal catalog snapshot.

Maintainer tooling, not part of the plugin contract: ``enumerate_bdp`` crawls
the full BPstat hierarchy (ids + terse English labels), then a bilingual
``/series/`` enrichment pass overlays the rich EN + PT descriptions (EN as the
primary search signal, PT folded in for Portuguese recall on the BM25 index).
The enriched rows become catalog entities and the catalog is indexed and built.
"""

from __future__ import annotations

from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.catalog.source import entities_from_raw

from parsimony_bdp.connectors._catalog import apply_enrichment, series_id_from_code
from parsimony_bdp.connectors.enumerate import enumerate_bdp
from parsimony_bdp.enrich import fetch_series_metadata
from parsimony_bdp.outputs import BDP_ENUMERATE_OUTPUT

CATALOG_NAMESPACE = "bdp"


async def build_bdp_catalog(*, enrich: bool = True) -> Catalog:
    """Enumerate the full BdP universe and build a searchable catalog snapshot.

    ``enrich`` (default on) runs the bilingual ``/series/`` metadata pass; set it
    ``False`` for a fast crawl-only smoke build (English labels only).
    """
    result = enumerate_bdp()
    df = result.data

    if enrich and not df.empty:
        series_ids = [
            sid
            for code in df.loc[df["entity_type"] == "series", "code"].tolist()
            if (sid := series_id_from_code(str(code))) is not None
        ]
        enrich_en = await fetch_series_metadata(series_ids, lang="EN")
        enrich_pt = await fetch_series_metadata(series_ids, lang="PT")
        df = apply_enrichment(df, enrich_en=enrich_en, enrich_pt=enrich_pt)

    entries = entities_from_raw(df, BDP_ENUMERATE_OUTPUT)
    catalog = Catalog(CATALOG_NAMESPACE, indexes=discovery_indexes(entries), default_field="title")
    catalog.set_entities(entries)
    catalog.build()
    return catalog


__all__ = ["CATALOG_NAMESPACE", "build_bdp_catalog"]
