# parsimony-bdp

Banco de Portugal connector — Portuguese macroeconomic, monetary, financial, and
external time series via the BPstat (JSON-stat) API.

Part of the [parsimony-connectors](https://github.com/ockham-sh/parsimony-connectors) monorepo. Distributed standalone on PyPI as `parsimony-bdp`.

## Connectors

| Name | Kind | Description |
|---|---|---|
| `bdp_fetch` | connector | Fetch Banco de Portugal observations by `domain_id` + `dataset_id` (optional `series_ids` filter and date window). |
| `enumerate_bdp` | enumerator | Crawl the full BPstat universe (~72 K series across 212 datasets / 65 leaf domains) for catalog discovery. |
| `bdp_search` | connector | Semantic-search the published BdP catalog snapshot; returns ranked series codes. |

## Install

```bash
pip install parsimony-bdp
```

Pulls in `parsimony-core>=0.7,<0.8` automatically. Verify discovery:

```bash
python -c "from parsimony import discover; print([p.name for p in discover.iter_providers()])"
```

## Configuration

No configuration required — the BPstat API is open and unauthenticated.

`bdp_search` reads a published catalog snapshot (default `hf://parsimony-dev/bdp`).
Override the snapshot location with the `PARSIMONY_BDP_CATALOG_URL` environment
variable, or pass `catalog_url=` at call time.

## Quick start

```python
from parsimony_bdp import CONNECTORS

# Discover via bdp_search, then fetch. A search hit's code splits as
# domain_id:dataset_id:series_id.
hits = CONNECTORS["bdp_search"](query="economic activity coincident indicator")
code = hits.raw.iloc[0]["code"]          # e.g. "48:aea9…:12099329"
domain_id, dataset_id, series_id = code.split(":")
result = CONNECTORS["bdp_fetch"](
    domain_id=int(domain_id), dataset_id=dataset_id, series_ids=series_id
)
print(result.raw.head())
```

For multi-plugin composition (autoloads everything installed):

```python
from parsimony import discover
connectors = discover.load_all()
```

## Catalogs

BPstat has no flat "list all series" endpoint, so `enumerate_bdp` walks the
`domain → dataset → series` hierarchy. Two things make the crawl both complete
and cheap: it **paginates the datasets list** (domains with >10 datasets would
otherwise lose series), and it crawls each dataset at `page_size=100&obs_last_n=1`
(100 series per page with a one-point observation array) — about 720 lean pages
for the whole universe. Each dataset is self-checked against its declared series
count. Maintainers then run a bilingual `/series/` enrichment pass (English +
Portuguese descriptions folded into the catalog `description` for cross-language
recall), build a `Catalog` snapshot (`scripts/build_catalog.py`), and push it to
the snapshot URL `bdp_search` reads — the build runs offline as a publish job,
never at query time.

## Provider

- Homepage: https://www.bportugal.pt
- BPstat portal: https://bpstat.bportugal.pt
- API docs: https://bpstat.bportugal.pt/data/docs

## License

See [LICENSE](./LICENSE).
