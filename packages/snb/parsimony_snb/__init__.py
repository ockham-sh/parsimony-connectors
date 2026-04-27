"""Swiss National Bank (SNB): fetch + catalog enumeration.

Data portal: https://data.snb.ch
No authentication required.
"""

from __future__ import annotations

import asyncio
import io
import logging
import re
from itertools import product
from typing import Annotated, Any

import httpx
import pandas as pd
from parsimony.connector import Connectors, connector, enumerator
from parsimony.errors import EmptyDataError
from parsimony.result import (
    Column,
    ColumnRole,
    OutputConfig,
    Provenance,
    Result,
)
from parsimony.transport import HttpClient, map_http_error
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

_BASE_URL = "https://data.snb.ch"


# SNB cube registry, harvested from the data portal's own navigation tree
# (``/json/structure/getNavigationTree``) for each publication and
# warehouse topic. Discovery URL set: ``/json/topic/getTopicsWithRootSubTopics``
# enumerates the seven publication topics (snb/banken/ziredev/finma/uvo/aube/cross)
# and seven warehouse topics (BSTA/KRED/SNB1A/DDUM/WKI/ZAST/ZAHL); each
# nav-tree call returns a recursive ``{title, cubeId, children}`` tree
# whose leaves with ``cubeId`` are the addressable cubes this connector
# can fetch via ``/api/cube/{id}/data/csv/en``. We keep a frozen tuple
# rather than redoing discovery on every catalog refresh because (a) cube
# IDs are stable across years on the SNB portal, (b) Treasury's catalog
# uses the same static-list pattern, and (c) shipping a curated list keeps
# the catalog reproducible across refreshes regardless of upstream
# transient errors. ``scripts/discover_cubes.py`` reproduces this list end
# to end if SNB ever ships new cubes.
#
# Verified live: 237/237 of these returned HTTP 200 from
# ``/api/cube/{id}/dimensions/en`` at audit time. Warehouse cubes whose
# IDs contain ``@`` or ``.`` (e.g. ``BSTA@SNB.AUR_U.ODF``) are *not*
# included — those are SDMX-style series exposed through ``json/list/...``,
# not the cube CSV API this connector wraps.
_KNOWN_CUBES: tuple[tuple[str, str], ...] = (
    ("amarbma", "Labour market"),
    ("ambeschkla", "Employees by economic activity"),
    ("amerwerb", "Employed persons"),
    ("ausshalam", "Foreign trade by country"),
    ("ausshawarm", "Foreign trade by goods category"),
    ("auvekoma", "Components (Year)"),
    ("auvekomq", "Components (Quarter)"),
    ("auvercurra", "Breakdown by currency (Year)"),
    ("auvercurrq", "Breakdown by currency (Quarter)"),
    ("auverdebta", "Switzerland’s external debt (Year)"),
    ("auverdeptq", "Switzerland’s external debt (Quarter)"),
    ("auverseca", "Breakdown by sector (Year)"),
    ("auversecq", "Breakdown by sector (Quarter)"),
    ("auvezeba", "Breakdown of changes in stocks (Year)"),
    ("auvezebq", "Breakdown of changes in stocks (Quarter)"),
    ("babasel", "Capital data Basel III (‘all banks’ regime) until 2019"),
    ("babilapoka", "Assets by currency"),
    ("babilapoua", "Assets – Annual"),
    ("babilapoum", "Assets – Monthly"),
    ("babilfalba", "by maturity (Annual)"),
    ("babilfalbm", "by maturity (Monthly)"),
    ("babilfalka", "By maturity"),
    ("babilfalua", "By maturity (Annual)"),
    ("babilfalum", "By maturity (Monthly)"),
    ("babilhypfibvua", "By lending group and interest"),
    ("babilhypisdpua", "By location of property"),
    ("babilkunddoua", "Customer deposits by domicile of domestic customer"),
    ("babillandua", "by selected country"),
    ("babilpoba", "by currency (Annual)"),
    ("babilpobgba", "by bank category"),
    ("babilpobgka", "By bank category"),
    ("babilpobgua", "By bank category"),
    ("babilpobm", "by currency (Monthly)"),
    ("babilppoka", "Liabilities by currency"),
    ("babilppoua", "Liabilities – Annual"),
    ("babilppoum", "Liabilities – Monthly"),
    ("babilsekum", "By domestic sector"),
    ("baerfgewverlua", "Appropriation of profit and coverage of losses"),
    ("baerfrechka", "Income statement items"),
    ("baerfrechua", "Income statement items"),
    ("bafovekreeinaus", "Claims and liabilities arising from loans and deposits abroad"),
    ("bahypoakredq", "Number of loans and credit volume"),
    ("bahypokebeq", "Indicators on lending value and affordability"),
    ("bahypopfeikrq", "Value of pledged property and borrowers’ income"),
    ("bakredbetgrbm", "Domestic loans by company size"),
    ("bakredinausbm", "Mortgage loans and other loans"),
    ("bakredsekbm", "Domestic loans by bank category"),
    ("bakredsekbrlzm", "Domestic loans by maturity"),
    ("bakredsekbrm", "Domestic loans by industry/economic activity"),
    ("bamire", "Minimum reserves"),
    ("baodfua", "Type of derivative, contract volumes and replacement values"),
    ("bastdagsua", "Offices"),
    ("bastdapersbua", "Number of staff"),
    ("bastrbwa", "Key figures"),
    ("bastrbwba", "Key figures"),
    ("bastrbwka", "Key figures"),
    ("batbtfk", "Capital data of systemically important banks and financial groups (TBTF regime) until 2019"),
    ("batbtfu", "Capital data of systemically important banks and financial groups (TBTF regime) until 2019"),
    ("batreuhba", "by currency (Annual)"),
    ("batreuhbm", "by currency (Monthly)"),
    ("batreuhka", "Fiduciary items by currency"),
    ("batreuhlandua", "by selected country"),
    ("batreuhua", "By currency (Annual)"),
    ("batreuhum", "By currency (Monthly)"),
    ("bawebedomsecwa", "By domicile of custody account holder and issuer, business sector and investment currency (Monthly)"),  # noqa: E501
    ("bawebedomsecwja", "By domicile of custody account holder and issuer, business sector and investment currency (Annual)"),  # noqa: E501
    ("bawebesec", "By domicile and business sector of custody account holder, security category (Monthly)"),
    ("bawebesecja", "By domicile and business sector of custody account holder, security category (Annual)"),
    ("bawebewa", "By domicile of custody account holder and issuer, security category and investment currency (Monthly)"),  # noqa: E501
    ("bawebewja", "By domicile of custody account holder and issuer, security category and investment currency (Annual)"),  # noqa: E501
    ("bopcapbala", "Financial account (Year)"),
    ("bopcapbalq", "Financial account (Quarter)"),
    ("bopcurra", "Current account (Year)"),
    ("bopcurrq", "Current account (Quarter)"),
    ("bopmercata", "Goods sales, by goods category (Year)"),
    ("bopmercatq", "Goods sales, by goods category (Quarter)"),
    ("bopmercoua", "Goods purchases/sales, by country (Year)"),
    ("bopmercouq", "Goods purchases/sales, by country (Quarter)"),
    ("bopovera", "Overview ‒ Year"),
    ("bopoverq", "Overview ‒ Quarter"),
    ("bopserva", "Services, by country (Year)"),
    ("bopservq", "Services, by country (Quarter)"),
    ("capchstocki", "Swiss stock indices"),
    ("capcollcat", "By investment categories"),
    ("capcollch", "By fund type"),
    ("capcollvf", "Claims and liabilities"),
    ("capforeignstocki", "Foreign stock indices"),
    ("caplqifcat", "By investment categories"),
    ("caplqifvf", "Claims and liabilities"),
    ("capmabond", "Capital market borrowing of CHF bond issues"),
    ("capmovshare", "Capital movements in shares of domestic companies ‒ by type of transaction"),
    ("capweums", "Securities turnover on Swiss stock exchange"),
    ("concon", "Consumer confidence (Quarter)"),
    ("conconm", "Consumer confidence (Month)"),
    ("conretail", "Retail turnover"),
    ("contourisma", "Tourism in Switzerland (Year)"),
    ("contourismm", "Tourism in Switzerland (Month)"),
    ("ddumfxcp", "Instruments, counterparties"),
    ("ddumfxcr", "Currencies"),
    ("ddumfxd", "Currency pairs, instruments, counterparties"),
    ("ddumfxir", "Instruments, counterparties"),
    ("ddumircp", "Instruments, counterparties"),
    ("ddumird", "Currencies, instruments, counterparties"),
    ("devkua", "Foreign exchange rates (Year)"),
    ("devkuhism", "Historical exchange rates for selected euro member countries (Month)"),
    ("devkuhistld", "Historical exchange rates for selected euro member countries (Day)"),
    ("devkum", "Foreign exchange rates (Month)"),
    ("devlanda", "Exchange rate indices ‒ 2001 methodology (up to March 2018) (Year)"),
    ("devlandm", "Exchange rate indices ‒ 2001 methodology (up to March 2018) (Month)"),
    ("devlandq", "Exchange rate indices ‒ 2001 methodology (up to March 2018) (Quarter)"),
    ("devwkibiia", "Bilateral indices (Year)"),
    ("devwkibiid", "Bilateral indices (Day)"),
    ("devwkibiim", "Bilateral indices (Month)"),
    ("devwkibiiq", "Bilateral indices (Quarter)"),
    ("devwkieffia", "Effective indices (Year)"),
    ("devwkieffid", "Effective indices (Day)"),
    ("devwkieffim", "Effective indices (Month)"),
    ("devwkieffiq", "Effective indices (Quarter)"),
    ("devwkilandea", "Euro area index"),
    ("devwkilandga", "Overall index"),
    ("fdiassliaufia", "Capital transactions – reconciliation with financial account"),
    ("fdiassliauini", "Investment income – reconciliation with current account"),
    ("fdiassliauipp", "Capital stocks – reconciliation with international investment position"),
    ("fdiausbabsa", "Capital stocks – by type of capital and by economic activity"),
    ("fdiausbla", "Capital stocks – by country and country group"),
    ("fdiauseabsa", "Investment income – by type of capital and by economic activity"),
    ("fdiauselanda", "Investment income – by country and country group"),
    ("fdiaustabsa", "Capital transactions – by type of capital and by economic activity"),
    ("fdiaustlanda", "Capital transactions – by country and country group"),
    ("fdichbabsa", "Capital stocks – by type of capital and by economic activity"),
    ("fdichbinvla", "Capital stocks – by investor level, country and country group"),
    ("fdicheabsa", "Investment income – by type of capital and by economic activity"),
    ("fdicheinvla", "Investment income – by investor level, country and country group"),
    ("fdichtabsa", "Capital transactions – by type of capital and by economic activity"),
    ("fdichtlanda", "Capital transactions – by country and country group"),
    ("frekgbgpa", "Deposits and loans of commercial banks, by counterparty (Year)"),
    ("frekgbgpaq", "Deposits and loans of commercial banks, by counterparty (Quarter)"),
    ("frfidodge", "Stocks and transactions – Year"),
    ("frfidodgeq", "Stocks and transactions – Quarter"),
    ("frsekfutgpa", "Financial assets and liabilities of financial corporations, by counterparty (Year)"),
    ("frsekfutgpaq", "Financial assets and liabilities of financial corporations, by counterparty (Quarter)"),
    ("frsekfutsek", "Stocks and flows – Year"),
    ("frsekfutsekq", "Stocks and flows – Quarter"),
    ("frsekgevehup", "Stocks – Year"),
    ("frsekgevehupq", "Stocks – Quarter"),
    ("frseknfu", "Stocks and flows – Year"),
    ("frseknfuq", "Stocks and flows – Quarter"),
    ("frsekphupooe", "Stocks and flows – Year"),
    ("frsekphupooeq", "Stocks and flows – Quarter"),
    ("frseksttsek", "Stocks and flows – Year"),
    ("frseksttsekq", "Stocks and flows – Quarter"),
    ("gdpap", "Gross domestic product by type of expenditure – nominal"),
    ("gdpgnp", "Gross domestic product by type of income and gross national income – nominal"),
    ("gdppn", "Gross domestic product by type of production – nominal"),
    ("gdppr", "Gross domestic product by type of production – real"),
    ("gdprpq", "Gross domestic product by type of expenditure – real"),
    ("indumem", "New orders and turnover in the mechanical and electrical engineering industries"),
    ("indusect", "Statistics on production and turnover in industry, by economic activity"),
    ("iualoaus", "Unemployment abroad"),
    ("iucurracpa", "Current accounts of major trading partners"),
    ("iugdphp", "Gross domestic product of major trading partners"),
    ("iukpaus", "Consumer prices abroad"),
    ("opanmuauspbs", "Number of staff – by economic activity"),
    ("opanmuauspland", "Number of staff – by country and country group"),
    ("opanmuausubs", "Number of companies – by economic activity"),
    ("opanmuausuland", "Number of companies – by country and country group"),
    ("opanmuausumbs", "Turnover – by economic activity"),
    ("opanmuausumland", "Turnover – by country and country group"),
    ("opanmumkpbs", "Number of staff – by source, economic activity"),
    ("pledelropr", "Prices of precious metals and raw materials"),
    ("plimoincha", "Total for Switzerland (Year)"),
    ("plimoinchq", "Total for Switzerland (Quarter)"),
    ("plimoingema", "Municipality types (Year)"),
    ("plimoingemq", "Municipality types (Quarter)"),
    ("plimoinreg", "By market area (Year)"),
    ("plimoinregq", "By market area (Quarter)"),
    ("plkopr", "Consumer prices (Total)"),
    ("plkoprart", "Type and origin of products"),
    ("plkoprex", "Additional classifications"),
    ("plkoprgru", "Major groups"),
    ("plkoprinfla", "SNB and SFSO core inflation rates"),
    ("pllohnind", "Salary/wage indices"),
    ("plproimpr", "Producer and import prices"),
    ("pubfin", "Public finances"),
    ("rendeiduebd", "Spot interest rates on Swiss Confederation bonds, euro area government bonds and CHF bond issues for various borrower categories – Day"),  # noqa: E501
    ("rendeiduebm", "Spot interest rates on Swiss Confederation bonds, euro area government bonds and CHF bond issues for various borrower categories – Month"),  # noqa: E501
    ("rendoblid", "Yields on bond issues ‒ 2002 methodology (up to July 2025) (Day)"),
    ("rendoblim", "Yields on bond issues ‒ 2002 methodology (up to July 2025) (Month)"),
    ("rendoeid", "Yields to maturity and residual maturities of individual Swiss Confederation bond issues"),
    ("rendopar", "Parameters"),
    ("sddsbop36912q", "Balance of Payments"),
    ("sddscbs14m", "Central Bank Survey"),
    ("sddsdcs30m", "Depository Corporations Survey"),
    ("sddsdse14710q", "Debt Securities"),
    ("sddsexd36912q", "External Debt"),
    ("sddsfsi36912q", "Financial Soundness Indicators"),
    ("sddsiip36912q", "International Investment Position"),
    ("sddsilv7m", "Official Reserve Assets"),
    ("sddssbs14710q", "Sectoral Balance Sheets"),
    ("snbband", "Target range of the SNB (until July 2019)"),
    ("snbbipo", "Balance sheet items of the SNB"),
    ("snbcurrc", "By currency"),
    ("snbcurrinvc", "Investment categories and ratings"),
    ("snbcurrp", "Currency breakdown"),
    ("snbfxtr", "Foreign exchange transactions"),
    ("snbgwdchfsgw", "Sight deposits in Swiss francs at the SNB"),
    ("snbgwdmigirow", "Minimum reserves: sight deposits"),
    ("snbgwdzid", "Interest rates and threshold factor"),
    ("snbimfcnd", "Contingent short-term net drains on foreign currency assets"),
    ("snbimfmi", "Memo items"),
    ("snbimfpnd", "Predetermined short-term net drains on foreign currency assets"),
    ("snbimfra", "Official reserve assets and other foreign currency assets"),
    ("snbiproga", "SNB conditional inflation forecast (Year)"),
    ("snbiprogq", "SNB conditional inflation forecast (Quarter)"),
    ("snbkosiq", "Business cycle signals"),
    ("snbmoba", "Origination/Utilisation"),
    ("snbmonagg", "M1, M2 and M3"),
    ("snbnomu", "Banknotes and coins in circulation"),
    ("snboffzisa", "Official interest rates"),
    ("zavegelade", "E-money ‒ loading and float"),
    ("zavesic", "Payment transactions via Swiss Interbank Clearing (SIC)"),
    ("zavezaka", "Number of payment cards and ATMs"),
    ("zavezaluba", "Payments and cash withdrawals"),
    ("zavkuzaart", "Outgoing payments"),
    ("zavkuzawae", "Outgoing payments"),
    ("zavkuzeart", "Incoming payments"),
    ("zavkuzewae", "Incoming payments"),
    ("zikreddet", "By product"),
    ("zikredkla", "By product and credit risk category"),
    ("zikredlauf", "By product and maturity"),
    ("zikredvol", "By product and loan amount"),
    ("zikrepro", "Published interest rates for new business"),
    ("zimoma", "Money market rates"),
    ("zipfanda", "Mortgage bond issues by rate of interest and average rate of interest"),
    ("zirepo", "Repo reference rates"),
    ("ziverza", "Average interest rates of selected balance sheet items (Annual)"),
    ("ziverzq", "Average interest rates of selected balance sheet items (Quarterly)"),
)

# Cap on series rows emitted per cube. SNB exposes 9 mega-cubes whose
# dimension cartesian-product exceeds 2,000 (e.g. ``frsekfutsek`` at
# 5,040, ``babilsekum`` at 3,168) — the leaves are mostly redundant
# currency × counterpart × maturity crossings that drown out useful
# semantic signal in the embedder. Above this threshold we collapse to a
# single cube-level row so the cube remains discoverable but the catalog
# stays under ~5K rows. Cubes at or below the cap (228 of 237 at audit
# time) emit one row per series exactly as before.
_MAX_SERIES_PER_CUBE = 100

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "Interest rates": ["zins", "interest", "rate", "libor", "saron", "yield", "bond"],
    "Exchange rates": ["kurs", "exchange", "devisen", "wechsel", "currency", "foreign exchange"],
    "Monetary aggregates": ["geldmenge", "monetary", "aggregat"],
    "Balance of payments": ["zahlungsbilanz", "balance of payment", "payment"],
    "Banking statistics": ["bank", "kredit", "credit", "bilanz"],
    "Securities": ["wertpapier", "securit", "obligation"],
    "Prices": ["preis", "price", "index", "consumer", "producer"],
    "National accounts": ["volkswirtschaft", "national account", "bip", "gdp"],
    "Reserves": ["reserve", "gold"],
    "Trade": ["handel", "trade", "aussenhandel"],
}


# ---------------------------------------------------------------------------
# Parameter models
# ---------------------------------------------------------------------------


class SnbFetchParams(BaseModel):
    """Parameters for fetching SNB data from a cube.

    The SNB exposes named series within a cube via the ``/dimensions``
    endpoint; the catalog encodes every (cube, series) pair as a compound
    code ``{cube_id}#{dim_path}`` (for example ``rendoblim#10J`` or
    ``devkum#M0.USD1``). Agents split on ``#`` and pass the right side as
    ``dim_sel`` to retrieve that single series — this connector accepts
    both the raw cube_id and an optional dim_sel exactly as before, so
    the catalog change is backwards-compatible with existing callers.
    """

    cube_id: Annotated[str, "ns:snb"] = Field(..., description="SNB cube identifier (e.g. rendoblim, devkum)")
    from_date: str | None = Field(default=None, description="Start date (YYYY or YYYY-MM or YYYY-MM-DD)")
    to_date: str | None = Field(default=None, description="End date (YYYY or YYYY-MM or YYYY-MM-DD)")
    dim_sel: str | None = Field(default=None, description="Dimension selection (e.g. D0(V0,V1),D1(ALL))")
    lang: str = Field(default="en", description="Language: en, de, fr, it")

    @field_validator("cube_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("cube_id must be non-empty")
        return v


class SnbEnumerateParams(BaseModel):
    """No parameters needed — enumerates all SNB series across known cubes."""

    pass


# ---------------------------------------------------------------------------
# Output configs
# ---------------------------------------------------------------------------

# Compound code ``{cube_id}#{series_key}`` so every addressable SNB time
# series has a unique catalog entry; agents split on ``#`` to recover the
# fetchable cube_id and the dimension selection. Mirrors the Treasury
# ``{endpoint}#{field}`` scheme so dispatchers can treat both providers
# uniformly.
SNB_ENUMERATE_OUTPUT = OutputConfig(
    columns=[
        Column(name="code", role=ColumnRole.KEY, namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        # ``description`` synthesised from cube_title + dimension path so
        # the embedder sees the human-readable series identity (e.g.
        # "10 year — Yields on Swiss Confederation bonds"). Routing through
        # DESCRIPTION (not METADATA) lifts it into ``semantic_text()``.
        Column(name="description", role=ColumnRole.DESCRIPTION),
        # ``source`` tells dispatchers which fetch connector handles this
        # entry. Treasury catalog uses the same column for the same
        # purpose.
        Column(name="source", role=ColumnRole.METADATA),
        Column(name="cube_id", role=ColumnRole.METADATA),
        Column(name="series_key", role=ColumnRole.METADATA),
        Column(name="dimension_path", role=ColumnRole.METADATA),
        Column(name="cube_title", role=ColumnRole.METADATA),
        Column(name="category", role=ColumnRole.METADATA),
        Column(name="frequency", role=ColumnRole.METADATA),
    ]
)

SNB_FETCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="cube_id", role=ColumnRole.KEY, param_key="cube_id", namespace="snb"),
        Column(name="title", role=ColumnRole.TITLE),
        Column(name="date", dtype="datetime", role=ColumnRole.DATA),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _infer_category(cube_id: str, description: str) -> str:
    text = f"{cube_id} {description}".lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "Other"


def _infer_frequency_from_dates(dates: list[str]) -> str:
    if not dates:
        return "Unknown"
    sample = dates[0]
    if re.match(r"^\d{4}$", sample):
        return "Annual"
    if re.match(r"^\d{4}-Q\d$", sample):
        return "Quarterly"
    if re.match(r"^\d{4}-\d{2}$", sample):
        return "Monthly"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", sample):
        return "Daily"
    return "Unknown"


_DURATION_TO_FREQ: dict[str, str] = {
    "P1D": "Daily",
    "P1M": "Monthly",
    "P3M": "Quarterly",
    "P1Y": "Annual",
}


def _parse_snb_csv(text: str) -> pd.DataFrame:
    """Parse SNB CSV response, skipping metadata preamble.

    Returns the data as a clean DataFrame with the first column as
    the date index — no melting.  Columns retain their original names.
    """
    # Strip BOM if present
    if text.startswith("﻿"):
        text = text[1:]

    lines = text.strip().split("\n")
    sep = ";" if ";" in text else ","

    # Find header line (first line with 2+ separators)
    header_idx = 0
    for i, line in enumerate(lines):
        if line.count(sep) >= 2:
            header_idx = i
            break

    data_text = "\n".join(lines[header_idx:])
    try:
        df = pd.read_csv(io.StringIO(data_text), sep=sep, dtype=str)
    except Exception as exc:
        logger.warning("Failed to parse SNB CSV: %s", exc)
        return pd.DataFrame()

    if df.empty:
        return df

    # First column is the date
    date_col = df.columns[0]
    df = df.rename(columns={date_col: "date"})

    # Convert value columns to numeric
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Series enumeration — descend the dimensions tree to leaf items
# ---------------------------------------------------------------------------


def _is_measure_series(item: dict[str, Any]) -> bool:
    """Whether ``item`` is an addressable time series rather than a grouping label.

    The SNB ``/dimensions`` response is a tree: top-level entries describe
    the dimensions of the cube; each carries ``dimensionItems`` which
    recursively contain either grouping nodes (themselves with
    ``dimensionItems``) or leaf items that name a single addressable
    coordinate along that dimension. A *measure* series is the cartesian
    product of one leaf per dimension — what the SNB CSV emits as a
    distinct ``Date;D0;D1;Value`` triple.

    A raw dimension item is a candidate measure iff it has an ``id`` and
    no nested ``dimensionItems``. Mirrors Treasury's ``_is_measure_field``:
    structural exclusion of grouping/label nodes, never silent inclusion.
    """
    if not isinstance(item, dict):
        return False
    if not item.get("id"):
        return False
    children = item.get("dimensionItems")
    return not (isinstance(children, list) and children)


def _collect_dimension_leaves(
    items: list[dict[str, Any]],
    parent_labels: tuple[str, ...] = (),
) -> list[tuple[str, tuple[str, ...]]]:
    """Walk a dimension's tree, returning ``(leaf_id, label_path)`` pairs.

    ``label_path`` accumulates the human-readable names from grouping
    ancestors (e.g. ``("Currency", "Europe", "EUR 1")``) so the emitted
    catalog row can carry a complete dimension path in its description
    without requiring callers to walk the tree themselves.
    """
    out: list[tuple[str, tuple[str, ...]]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("id") or ""
        if _is_measure_series(item):
            out.append((str(item["id"]), parent_labels + (name,)))
            continue
        # Grouping node: recurse with the group's own name appended.
        children = item.get("dimensionItems")
        if isinstance(children, list) and children:
            out.extend(_collect_dimension_leaves(children, parent_labels + (name,)))
    return out


def _series_from_dimensions(
    cube_id: str,
    cube_title: str,
    dimensions_payload: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Cartesian-product the cube's dimension leaves into one row per series.

    Returns a list of catalog rows ready for the enumerator. Each row's
    ``code`` is ``{cube_id}#{leaf_path}`` where ``leaf_path`` joins the
    chosen leaf id from each dimension with ``.`` — a stable scheme that
    survives both single- and multi-dimensional cubes (``rendoblim#10J``,
    ``devkum#M0.USD1``).

    When the dimensions payload is missing, malformed, or has no leaves,
    falls back to a single ``{cube_id}#`` entry so the cube is still
    reachable in the catalog.
    """
    rows: list[dict[str, str]] = []
    cube_name = (
        (dimensions_payload or {}).get("name")
        or (dimensions_payload or {}).get("cubeName")
        or cube_title
    )
    category = _infer_category(cube_id, cube_title)

    dims = (dimensions_payload or {}).get("dimensions") or []
    # Each entry in ``per_dim_leaves`` is the list of (leaf_id, label_path)
    # tuples for one dimension; we cartesian-product them to get series.
    per_dim_leaves: list[list[tuple[str, tuple[str, ...]]]] = []
    for dim in dims:
        if not isinstance(dim, dict):
            continue
        leaves = _collect_dimension_leaves(dim.get("dimensionItems") or [])
        if leaves:
            per_dim_leaves.append(leaves)

    def _cube_level_row() -> dict[str, str]:
        return {
            "code": f"{cube_id}#",
            "title": cube_name,
            "description": cube_title,
            "source": "snb_data_portal",
            "cube_id": cube_id,
            "series_key": "",
            "dimension_path": "",
            "cube_title": cube_title,
            "category": category,
            "frequency": "Unknown",
        }

    if not per_dim_leaves:
        # Empty/unknown dimensions — emit a coarse cube-level entry so the
        # cube is still discoverable. Compound code reduces to "cube#"
        # which fetch can interpret as "no dim_sel".
        rows.append(_cube_level_row())
        return rows

    # Cap series-level expansion: SNB has ~9 mega-cubes whose cartesian
    # product exceeds 2k entries — emit a single cube-level row instead so
    # the catalog stays under ~5K rows total without losing the cube as a
    # search target. Mirrors the cardinality discipline Treasury enforces
    # via its hand-curated ``_TREASURY_RATE_FEEDS`` list.
    total = 1
    for leaves in per_dim_leaves:
        total *= len(leaves)
    if total > _MAX_SERIES_PER_CUBE:
        rows.append(_cube_level_row())
        return rows

    for combo in product(*per_dim_leaves):
        leaf_ids = [leaf_id for leaf_id, _ in combo]
        label_segments = [labels[-1] for _, labels in combo if labels]
        full_paths = [" / ".join(labels) for _, labels in combo if labels]
        series_key = ".".join(leaf_ids)
        dimension_path = " | ".join(full_paths)
        leaf_label = " / ".join(label_segments) if label_segments else series_key
        title = f"{leaf_label} — {cube_name}"
        # Rich description: cube context + full dimension breadcrumbs so
        # the embedder sees both the specific series and the cube it
        # belongs to (per gold-standard pattern in Treasury).
        description = (
            f"{cube_title}. {dimension_path}." if dimension_path else cube_title
        )
        rows.append(
            {
                "code": f"{cube_id}#{series_key}",
                "title": title,
                "description": description,
                "source": "snb_data_portal",
                "cube_id": cube_id,
                "series_key": series_key,
                "dimension_path": dimension_path,
                "cube_title": cube_title,
                "category": category,
                "frequency": "",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


@connector(output=SNB_FETCH_OUTPUT, tags=["macro", "ch"])
async def snb_fetch(params: SnbFetchParams) -> Result:
    """Fetch SNB cube data by cube_id.

    Returns the cube's time series as a DataFrame with date + value
    columns named by the SNB.  Column names are the original dimension
    labels from the cube.
    """
    http = HttpClient(_BASE_URL)

    req_params: dict[str, str] = {}
    if params.from_date:
        req_params["fromDate"] = params.from_date
    if params.to_date:
        req_params["toDate"] = params.to_date
    if params.dim_sel:
        req_params["dimSel"] = params.dim_sel

    response = await http.request(
        "GET",
        f"/api/cube/{params.cube_id}/data/csv/{params.lang}",
        params=req_params,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        map_http_error(exc, provider="snb", op_name="cube/data")

    df = _parse_snb_csv(response.text)
    if df.empty:
        raise EmptyDataError(provider="snb", message=f"No data returned for cube: {params.cube_id}")

    df["cube_id"] = params.cube_id
    df["title"] = params.cube_id

    # Fetch cube title from dimensions endpoint
    try:
        dim_resp = await http.request("GET", f"/api/cube/{params.cube_id}/dimensions/{params.lang}")
        if dim_resp.status_code == 200:
            dim_data = dim_resp.json()
            if isinstance(dim_data, dict):
                df["title"] = dim_data.get("name", dim_data.get("cubeName", params.cube_id))
        else:
            logger.debug("SNB dimensions endpoint returned %d for %s", dim_resp.status_code, params.cube_id)
    except Exception as exc:
        logger.warning("Could not fetch title for SNB cube %s: %s", params.cube_id, exc)

    return Result.from_dataframe(
        df,
        Provenance(
            source="snb",
            params={"cube_id": params.cube_id},
            properties={"source_url": f"https://data.snb.ch/en/topics/{params.cube_id}"},
        ),
    )


async def _probe_cube(
    client: httpx.AsyncClient,
    cube_id: str,
    cube_title: str,
    sem: asyncio.Semaphore | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """Fetch ``/dimensions/en`` and a frequency hint for ``cube_id``.

    Returns ``(dimensions_payload, frequency)``. When the cube is
    retired (404) or the response is the SNB error envelope
    (``{"message": "..."}``), returns ``(None, "Unknown")`` so the
    caller can decide whether to skip or emit a fallback entry.

    The optional ``sem`` caps simultaneous probes — at 237 cubes × two
    requests each, an unbounded fan-out can saturate the SNB CDN's
    per-IP connection pool and trigger transient 429s; the semaphore
    keeps us under the WAF radar while still finishing in seconds.
    """
    dim_payload: dict[str, Any] | None = None
    frequency = "Unknown"

    async def _gated_get(path: str, **kw: Any) -> httpx.Response | None:
        if sem is None:
            return await client.get(path, **kw)
        async with sem:
            return await client.get(path, **kw)

    try:
        dim_resp = await _gated_get(f"/api/cube/{cube_id}/dimensions/en")
        if dim_resp is not None and dim_resp.status_code == 200:
            try:
                payload = dim_resp.json()
            except ValueError:
                payload = None
            if isinstance(payload, dict) and "dimensions" in payload:
                dim_payload = payload
    except Exception as exc:
        logger.debug("SNB dimensions probe failed for %s: %s", cube_id, exc)

    # Frequency inference is best-effort; sample one CSV page.
    try:
        data_resp = await _gated_get(
            f"/api/cube/{cube_id}/data/csv/en",
            params={"fromDate": "2020"},
        )
        if data_resp is not None and data_resp.status_code == 200:
            sep = ";" if ";" in data_resp.text else ","
            dates: list[str] = []
            for line in data_resp.text.strip().split("\n")[:50]:
                parts = line.split(sep)
                if parts and re.match(r"^\d{4}", parts[0].strip().strip('"')):
                    dates.append(parts[0].strip().strip('"'))
            frequency = _infer_frequency_from_dates(dates)
    except Exception as exc:
        logger.debug("SNB frequency probe failed for %s: %s", cube_id, exc)

    return dim_payload, frequency


@enumerator(
    output=SNB_ENUMERATE_OUTPUT,
    tags=["macro", "ch"],
)
async def enumerate_snb(params: SnbEnumerateParams) -> pd.DataFrame:
    """Enumerate every addressable SNB series across the curated cube list.

    Strategy: for each cube in :data:`_KNOWN_CUBES`, hit
    ``/api/cube/{id}/dimensions/en`` to learn the dimension tree, then
    cartesian-product the leaves to produce one catalog row per
    addressable time series. Emitting one row per (cube, series) pair —
    rather than one row per cube — surfaces the SNB's actual fetchable
    granularity (yield-curve maturities, currency pairs, etc.) to
    embedding-based search.

    Compound code is ``{cube_id}#{leaf_path}`` so an agent can split on
    ``#`` and pass the right-hand side as :attr:`SnbFetchParams.dim_sel`
    to retrieve that single series — same scheme as Treasury's
    ``{endpoint}#{field}``.

    Cubes that the SNB has retired (HTTP 404 / JSON error envelope)
    are skipped silently so the curated list can age gracefully.
    """
    rows: list[dict[str, str]] = []
    # Concurrency cap: at 237 cubes × two requests per cube the SNB CDN
    # has been observed to throttle unbounded fan-outs, so we keep the
    # in-flight request count modest. 20 keeps wall-time near the latency
    # floor (validated at ~5s for full-tree probe) without tripping the
    # WAF.
    sem = asyncio.Semaphore(20)
    async with httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0) as client:
        probes = await asyncio.gather(
            *(_probe_cube(client, cid, title, sem) for cid, title in _KNOWN_CUBES),
            return_exceptions=False,
        )

    for (cube_id, cube_title), (dim_payload, frequency) in zip(_KNOWN_CUBES, probes, strict=True):
        if dim_payload is None:
            # Retired cube — skip rather than emit a stale row.
            logger.debug("SNB cube %s has no dimensions payload; skipping", cube_id)
            continue
        cube_rows = _series_from_dimensions(cube_id, cube_title, dim_payload)
        for row in cube_rows:
            row["frequency"] = frequency
        rows.extend(cube_rows)

    columns = [
        "code",
        "title",
        "description",
        "source",
        "cube_id",
        "series_key",
        "dimension_path",
        "cube_title",
        "category",
        "frequency",
    ]
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

from parsimony_snb.search import (  # noqa: E402, F401  (after public decorators; re-exported)
    PARSIMONY_SNB_CATALOG_URL_ENV,
    SNB_SEARCH_OUTPUT,
    SnbSearchParams,
    snb_search,
)

CATALOGS: list[tuple[str, object]] = [("snb", enumerate_snb)]

CONNECTORS = Connectors([snb_fetch, enumerate_snb, snb_search])
