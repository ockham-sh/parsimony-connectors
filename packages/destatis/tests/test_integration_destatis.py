"""Live integration tests for parsimony-destatis (GENESIS-Online).

Hits the real public GENESIS-Online REST API
(``genesis.destatis.de/genesis/api/rest``). GENESIS is **keyless** (anonymous
access), so these tests need no env vars and run without secrets — there is no
key to bind and nothing that could leak (no ``assert_no_secret_leak`` needed).

Skipped by default — the root ``pyproject.toml`` sets ``-m 'not integration'``.
Run explicitly with::

    uv run pytest packages/destatis -m integration

**Bounded crawls only.** ``enumerate_destatis`` normally fans out ``1 + 2N``
requests over ALL ~331 GENESIS statistics; the live test monkeypatches the
statistics-index loader down to a single small statistic so the crawl fires
exactly three live requests (``/statistics`` is replaced; one ``/information``
+ one ``/tables``). ``destatis_search`` is covered against a locally-built
3-row fixture catalog rather than the published snapshot, so it never triggers
a cold full enumerate + embed.

GENESIS occasionally throttles anonymous access; where a verb is transiently
rate-limited live, the typed ``RateLimitError`` is accepted as a valid
documented outcome rather than a hard failure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from parsimony.catalog import Catalog
from parsimony.catalog.policy import discovery_indexes
from parsimony.errors import RateLimitError
from parsimony.result import Result
from parsimony_test_support import assert_provenance_shape

from parsimony_destatis.connectors import enumerate as enumerate_module
from parsimony_destatis.connectors.enumerate import enumerate_destatis
from parsimony_destatis.connectors.fetch import destatis_fetch
from parsimony_destatis.outputs import DESTATIS_ENUMERATE_OUTPUT
from parsimony_destatis.search import destatis_search

pytestmark = pytest.mark.integration

# A stable, high-traffic GENESIS table: Consumer Price Index, Germany, annual.
_KNOWN_TABLE = "61111-0001"
# A small statistic for the bounded enumerate crawl: "Feststellung des
# Gebietsstands" (territorial status) has only ~2 tables.
_SMALL_STAT = {
    "code": "11111",
    "name": {"de": "Feststellung des Gebietsstands", "en": "Recording of territorial status"},
}


def test_destatis_fetch_known_table_live() -> None:
    result = destatis_fetch(name=_KNOWN_TABLE)

    assert_provenance_shape(result, expected_source="destatis_fetch", required_param_keys=["name"])
    df = result.raw
    assert not df.empty, "GENESIS fetch returned an empty DataFrame"
    assert (df["series_id"] == _KNOWN_TABLE).all()
    # Real content, not just shape: values are real numbers, not all-NaN/constant.
    assert df["value"].dtype.kind == "f"
    assert df["value"].notna().any(), "no real observation values"
    assert df["value"].nunique() > 1, "values implausibly constant"
    # 61111-0001 carries both CPI index levels and year-on-year change rates,
    # so values span a wide but bounded band — sanity-check the magnitude
    # rather than assume a single positive price level.
    vals = df["value"].dropna()
    assert ((vals > -50) & (vals < 1000)).all(), f"CPI values out of plausible range: {vals.tolist()[:5]}"
    # Dates parse to real datetimes (coerced in destatis_fetch).
    assert df["date"].dtype.kind == "M"
    assert df["date"].notna().any(), "record dates all NaT"


def test_destatis_fetch_year_range_live() -> None:
    """A bounded year range still returns real CPI observations."""
    result = destatis_fetch(name=_KNOWN_TABLE, start_year="2015", end_year="2020")
    df = result.raw
    assert not df.empty
    assert df["value"].notna().any(), "no real values in the requested window"


# A reference-date population table: its time axis is the ``STAG`` dimension
# (keys like ``1999-12-31``), NOT ``JAHR``. The old name-based time detector
# fell back to dim 0 (the constant ``statistic`` dim) and emitted the statistic
# code ``12411`` as a "year" → ParseError. This locks the key-shape fix in live.
_REFERENCE_DATE_TABLE = "12411-0001"


def test_destatis_fetch_reference_date_table_live() -> None:
    """A STAG (reference-date) table fetches with real ISO dates — the headline
    time-dimension fix, verified against the live API (this whole class of table
    hard-failed before the fix).
    """
    result = destatis_fetch(name=_REFERENCE_DATE_TABLE)
    df = result.raw
    assert not df.empty, "reference-date table returned no rows"
    assert df["date"].dtype.kind == "M", "STAG dates did not parse to datetimes"
    assert df["date"].notna().any(), "all reference dates NaT"
    years = df["date"].dropna().dt.year
    # Real reference dates land in a plausible band — and crucially the statistic
    # code (12411) never leaks in as a bogus year.
    assert (years > 1800).all() and (years < 2100).all(), f"implausible years: {sorted(set(years))[:5]}"
    assert df["value"].notna().any(), "no real population values"


def test_enumerate_destatis_bounded_single_statistic_live(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Crawl ONE real statistic (territorial status, ~2 tables) to verify the
    live ``/information`` + ``/tables`` shapes without the full 331-statistic
    fan-out. The index loader is monkeypatched so only the per-statistic calls
    hit the network.
    """

    def _bounded_index(_fetcher: Any) -> list[dict[str, Any]]:
        return [_SMALL_STAT]

    monkeypatch.setattr(enumerate_module, "_load_statistics_index", _bounded_index)

    try:
        result = enumerate_destatis()
    except RateLimitError:
        pytest.skip("GENESIS transiently rate-limited the bounded enumerate crawl")

    df = result.raw
    # @enumerator enforces an EXACT column match against the declared schema.
    assert list(df.columns) == [c.name for c in DESTATIS_ENUMERATE_OUTPUT.columns]
    assert not df.empty, "bounded enumerate returned no rows"

    stat_rows = df[df["entity_type"] == "statistic"]
    table_rows = df[df["entity_type"] == "table"]
    assert len(stat_rows) == 1, "expected exactly one statistic row"
    assert len(table_rows) >= 1, "expected at least one table row"

    stat = stat_rows.iloc[0]
    assert stat["code"] == "11111"
    # Real content in the declared metadata columns — not just column names.
    assert stat["title"].strip(), "blank statistic title"
    assert stat["description"].strip(), "no real description prose"
    assert (df["source"] == "genesis_online").all(), "source routing metadata missing"
    # The /information description prose rides along from the real payload.
    assert df["description"].str.len().gt(0).all(), "blank descriptions"
    # Table rows carry real codes + titles.
    assert table_rows["code"].str.startswith("11111-").all(), "table codes not parsed"
    assert table_rows["title"].str.len().gt(0).all(), "blank table titles"
    # Variable metadata populated from the real variableCodes/variableNames
    # shape (this was always-empty before the fix).
    assert df["variable_codes"].str.len().gt(0).any(), "variable_codes all empty"
    assert df["variable_names_en"].str.len().gt(0).any(), "variable_names_en all empty"

    # build_entities round-trips on the real slice (the catalog-build entry point).
    entities = list(Result(raw=df, output_spec=DESTATIS_ENUMERATE_OUTPUT).entities.values())
    assert len(entities) == len(df)
    assert entities[0].namespace == "destatis"


def test_destatis_search_over_bounded_catalog_live(tmp_path: Path) -> None:
    """Exercise ``destatis_search`` end-to-end over a small, locally-built
    catalog so the search runs network-free and never triggers the expensive
    full ``build_destatis_catalog()`` fan-out.
    """
    rows = [
        {
            "code": "61111-0001",
            "title": "Consumer price index: Germany, annual",
            "description": (
                "Consumer price index (CPI). Verbraucherpreisindex. Measures the average "
                "change in prices of goods and services for private households. Inflation indicator."
            ),
            "entity_type": "table",
            "parent_statistic": "61111",
            "subject_area": "Consumer prices",
            "title_de": "Verbraucherpreisindex: Deutschland, Jahre",
            "title_en": "Consumer price index: Germany, annual",
            "variable_codes": "PREIS1,ZEIT",
            "variable_names_en": "Index,Time",
            "source": "genesis_online",
        },
        {
            "code": "12411-0001",
            "title": "Population: Germany, reference date",
            "description": (
                "Population by sex. Bevölkerung nach Geschlecht. Resident population of Germany "
                "at the reference date. Demographic statistics."
            ),
            "entity_type": "table",
            "parent_statistic": "12411",
            "subject_area": "Population",
            "title_de": "Bevölkerung: Deutschland, Stichtag",
            "title_en": "Population: Germany, reference date",
            "variable_codes": "BEVST,GES",
            "variable_names_en": "Population,Sex",
            "source": "genesis_online",
        },
        {
            "code": "13211-0001",
            "title": "Unemployment rate: Germany, monthly",
            "description": (
                "Unemployment rate. Arbeitslosenquote. Registered unemployed as a share of the "
                "labour force in Germany, monthly. Labour market statistics."
            ),
            "entity_type": "table",
            "parent_statistic": "13211",
            "subject_area": "Labour market",
            "title_de": "Arbeitslosenquote: Deutschland, Monate",
            "title_en": "Unemployment rate: Germany, monthly",
            "variable_codes": "ERWP01,ZEIT",
            "variable_names_en": "Unemployment,Time",
            "source": "genesis_online",
        },
    ]
    df = pd.DataFrame(rows, columns=[c.name for c in DESTATIS_ENUMERATE_OUTPUT.columns])
    entries = list(Result(raw=df, output_spec=DESTATIS_ENUMERATE_OUTPUT).entities.values())
    catalog = Catalog("destatis", indexes=discovery_indexes(entries))
    catalog.set_entities(entries)
    catalog.build()
    out_dir = tmp_path / "destatis_catalog"
    catalog.save(out_dir)

    result = destatis_search(query="consumer prices inflation", limit=5, catalog_url=str(out_dir))

    assert_provenance_shape(result, expected_source="destatis_search", required_param_keys=["query"])
    sdf = result.raw
    assert list(sdf.columns) == ["code", "title", "coverage", "score", "matched"]
    assert not sdf.empty, "search over the fixture catalog returned nothing"
    assert len(sdf) <= 3, "search returned more than the 3-row fixture catalog"
    # Real ranking: the CPI entry is the top hit and scores are populated.
    assert sdf.iloc[0]["code"] == "61111-0001"
    assert "price" in sdf.iloc[0]["title"].lower()
    assert sdf["score"].notna().all(), "search scores not populated"

    # Ranking actually discriminates: a different query surfaces a different
    # entry as the top hit (not the same row regardless of query).
    other = destatis_search(query="unemployment labour market", limit=5, catalog_url=str(out_dir))
    assert other.raw.iloc[0]["code"] == "13211-0001"
