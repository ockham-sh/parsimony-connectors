"""Happy-path tests for the Banco de España connectors.

BdE BIEST is public (no api_key); template 401/429 contract does not apply.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError

from parsimony_bde import (
    BDE_ENUMERATE_OUTPUT,
    CONNECTORS,
    BdeEnumerateParams,
    BdeFetchParams,
    bde_fetch,
    enumerate_bde,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bde_fetch", "enumerate_bde", "bde_search"}


# ---------------------------------------------------------------------------
# bde_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_bde_fetch_merges_single_series_response() -> None:
    respx.get("https://app.bde.es/bierest/resources/srdatosapp/listaSeries").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "serie": "D_1NBAF472",
                    "descripcionCorta": "Price index",
                    "codFrecuencia": "M",
                    "fechas": ["2026-01", "2026-02"],
                    "valores": ["108.4", "108.7"],
                }
            ],
        )
    )

    result = await bde_fetch(BdeFetchParams(key="D_1NBAF472"))

    assert result.provenance.source == "bde"
    df = result.data
    assert len(df) >= 1


@respx.mock
@pytest.mark.asyncio
async def test_bde_fetch_raises_empty_data_on_empty_list() -> None:
    respx.get("https://app.bde.es/bierest/resources/srdatosapp/listaSeries").mock(
        return_value=httpx.Response(200, json=[])
    )

    with pytest.raises(EmptyDataError):
        await bde_fetch(BdeFetchParams(key="XX"))


def test_fetch_rejects_invalid_time_range() -> None:
    with pytest.raises(ValueError, match="time_range"):
        BdeFetchParams(key="X", time_range="3M")


def test_fetch_rejects_empty_key() -> None:
    with pytest.raises(ValueError):
        BdeFetchParams(key="  ")


# ---------------------------------------------------------------------------
# enumerate_bde — catalog discovery via published CSV chapters
# ---------------------------------------------------------------------------


# Seven fake CSV chapters that mirror the real ``catalogo_*.csv`` shape: a
# 17-column header (with the canonical trailing space on ``Nombre de la serie``)
# and at least one data row whose first column is the API-fetchable series
# code. The ``ti`` chapter encodes a non-ASCII ``í`` to lock in CP1252
# decoding; if anyone swaps the decoder to UTF-8 in a refactor, the assertion
# on ``"Tipo de interés"`` will break.
def _csv(header: list[str], rows: list[list[str]]) -> str:
    """Render a quoted, comma-separated CSV the way BdE's exporter does."""
    import csv as _csv_mod
    import io

    buf = io.StringIO()
    writer = _csv_mod.writer(buf, quoting=_csv_mod.QUOTE_ALL)
    writer.writerow(header)
    for r in rows:
        writer.writerow(r)
    return buf.getvalue()


_HEADER = [
    "Nombre de la serie ",
    "Numero secuencial",
    "Alias de la serie",
    "Nombre del archivo con los valores de la serie",
    "Descripcion de la serie",
    "Tipo de variable",
    "Codigo de unidades",
    "Exponente",
    "Numero de decimales",
    "Descripcion de unidades y exponente",
    "Frecuencia de la serie",
    "Fecha de la primera observacion",
    "Fecha de la ultima observacion",
    "Numero de observaciones",
    "Titulo de la serie",
    "Fuente",
    "Notas",
]


def _row(
    *,
    serie: str,
    alias: str,
    description: str,
    frequency_raw: str,
    unit: str,
    decimals: str,
    start: str,
    end: str,
    nobs: str,
    title: str,
    source_org: str,
) -> list[str]:
    return [
        serie,
        "12345",
        alias,
        f"{alias.split('.')[0]}.csv",
        description,
        "MEDIA",
        unit,
        "0",
        decimals,
        unit,
        frequency_raw,
        start,
        end,
        nobs,
        title,
        source_org,
        "",
    ]


_CSV_BE = _csv(
    _HEADER,
    [
        _row(
            serie="DSPC102020WP31000_ES14A_ZU2_TSC.T",
            alias="BE_1_1.1",
            description="National accounts. Household final consumption expenditure.",
            frequency_raw="TRIMESTRAL",
            unit="Millones de euros",
            decimals="2",
            start="MAR 1995",
            end="DIC 2025",
            nobs="124",
            title="National Accounts/SEC2010/Household consumption",
            source_org="Instituto Nacional de Estadistica",
        ),
    ],
)
_CSV_PB = _csv(
    _HEADER,
    [
        _row(
            serie="PB_1_1.1",
            alias="PB_1_1.1",
            description="EPB net change in lending criteria to non-financial corporations.",
            frequency_raw="TRIMESTRAL",
            unit="Porcentaje",
            decimals="1",
            start="DIC 2002",
            end="DIC 2021",
            nobs="77",
            title="Bank Lending Survey/Lending criteria/NFCs",
            source_org="Banco de Espana",
        ),
    ],
)
_CSV_SI = _csv(
    _HEADER,
    [
        _row(
            serie="D_1KH90101",
            alias="SI_1_1.2",
            description="Consumer confidence index.",
            frequency_raw="MENSUAL",
            unit="Porcentaje neto",
            decimals="2",
            start="JUN 1986",
            end="MAR 2026",
            nobs="478",
            title="General Statistics/Opinion surveys/Consumer confidence",
            source_org="Banco de Espana a partir de Comision Europea",
        ),
    ],
)
_CSV_TC = _csv(
    _HEADER,
    [
        _row(
            serie="DTCCBCEUSDEUR.B",
            alias="TC_1_1.1",
            description="Exchange rate. US dollars per euro. Daily data",
            frequency_raw="LABORABLE",
            unit="Dolares de Estados Unidos por Euro",
            decimals="4",
            start="04 ENE 1999",
            end="23 ABR 2026",
            nobs="7124",
            title="Exchange rates/ECB spot rate/USD/EUR",
            source_org="BANCO CENTRAL EUROPEO",
        ),
    ],
)
# ``ti`` includes a non-ASCII character to exercise the CP1252 decoder.
_CSV_TI = _csv(
    _HEADER,
    [
        _row(
            serie="D_DTFK09A0",
            alias="TI_1_1.1",
            description="Tipo de interés. Operaciones de política monetaria del eurosistema.",
            frequency_raw="LABORABLE",
            unit="Porcentaje",
            decimals="3",
            start="01 ENE 1999",
            end="23 ABR 2026",
            nobs="7125",
            title="Monetary policy/Eurosystem operations/Fixed rate auctions",
            source_org="",
        ),
    ],
)
# ``cf`` = Financial Accounts of the Spanish Economy (CFEE, SEC2010). Faceted
# DSD path (every segment ``Facet: value``), so the parser hits the
# dataset-only code path where leaf is empty and description becomes title.
_CSV_CF = _csv(
    _HEADER,
    [
        _row(
            serie="DMZ10S0000Z.Q",
            alias="CF_2_10A.1",
            description="CFEE. SEC2010. Saldo. Todos los instrumentos. Activo de OIFM.",
            frequency_raw="TRIMESTRAL",
            unit="Miles de euros",
            decimals="0",
            start="DIC 1994",
            end="DIC 2025",
            nobs="125",
            title=(
                "Descripción de la DSD: Series de Cuentas Financieras/"
                "Metodología: SEC2010/Tipo de variable: Saldo/"
                "Sector activo: Otras instituciones financieras monetarias"
            ),
            source_org="Banco de España",
        ),
    ],
)
# ``ie`` = International Economy (world prices, commodity indices).
_CSV_IE = _csv(
    _HEADER,
    [
        _row(
            serie="D_1NEAD861",
            alias="IE_2_7.1",
            description=(
                "Economía internacional. Índice de precios internacionales "
                "de materias primas en euros."
            ),
            frequency_raw="MENSUAL",
            unit="Base 2000 = 100",
            decimals="2",
            start="ENE 1995",
            end="OCT 2021",
            nobs="322",
            title="ECONOMIA MUNDIAL/PRECIOS INTERNACIONALES/MATERIAS PRIMAS/EN EUROS",
            source_org="THE ECONOMIST",
        ),
    ],
)


def _mock_csv_chapters() -> None:
    payloads = {
        "be": _CSV_BE,
        "cf": _CSV_CF,
        "ie": _CSV_IE,
        "pb": _CSV_PB,
        "si": _CSV_SI,
        "tc": _CSV_TC,
        "ti": _CSV_TI,
    }
    base = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
    for chapter, body in payloads.items():
        respx.get(f"{base}/catalogo_{chapter}.csv").mock(
            return_value=httpx.Response(
                200,
                # Encode in CP1252 so the production decoder path is exercised.
                content=body.encode("cp1252"),
                headers={"content-type": "text/csv"},
            )
        )


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_pulls_all_seven_catalog_chapters() -> None:
    _mock_csv_chapters()
    result = await enumerate_bde(BdeEnumerateParams())
    df = result.data

    # One row per chapter mock — seven total — wired to BDE_ENUMERATE_OUTPUT.
    # CF (Financial Accounts of the Spanish Economy) and IE (International
    # Economy) were added after the initial 5-chapter ship when an exhaustive
    # probe of ``catalogo_{aa..zz}.csv`` uncovered them.
    assert len(df) == 7
    assert set(df["category"]) == {
        "General Statistics",
        "Financial Accounts",
        "International Economy",
        "Bank Lending Survey",
        "Financial Indicators",
        "Exchange Rates",
        "Interest Rates",
    }
    assert set(df["source"]) == {"bde_biest"}


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_includes_cf_financial_accounts_rows() -> None:
    """CF chapter (CFEE / Financial Accounts of the Spanish Economy) ships
    the bulk of SEC2010 sector balance-sheet series — ~4.7k rows live here
    and nowhere else. Regression guard: if someone removes ``cf`` from
    ``_CATALOG_CHAPTERS`` we lose those series silently."""
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    cf_rows = df[df["category"] == "Financial Accounts"]
    assert len(cf_rows) >= 1
    cf_row = cf_rows.iloc[0]
    # CF titles are faceted DSD paths; leaf is empty so title falls back to
    # the description (the only user-facing prose we have).
    assert "CFEE" in cf_row["title"] or "SEC2010" in cf_row["title"]
    # Faceted path lands in ``dataset`` METADATA, not ``title``.
    assert "SEC2010" in cf_row["dataset"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_includes_ie_international_economy_rows() -> None:
    """IE chapter (International Economy) carries world-price and
    commodity-index series. Most IE codes overlap with BE under a
    different taxonomy, but the category-filtered view still matters."""
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    ie_rows = df[df["category"] == "International Economy"]
    assert len(ie_rows) >= 1
    assert "ECONOMIA MUNDIAL" in ie_rows.iloc[0]["dataset"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_populates_description_column() -> None:
    """The DESCRIPTION column carries upstream long-form prose so the embedder
    sees full sentences at index time, not just the leaf title."""
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    ti_row = df[df["key"] == "D_DTFK09A0"].iloc[0]
    # Spanish character round-trips through CP1252 → str unscathed.
    assert "Tipo de interés" in ti_row["description"]
    # No description column row is empty — every series ships with one.
    assert (df["description"].str.len() > 0).all()


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_carries_source_metadata_for_dispatch() -> None:
    """Every catalog row carries ``source`` so an agent dispatching off a
    search hit knows which fetch connector to call without parsing the key."""
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    assert "source" in df.columns
    assert (df["source"] == "bde_biest").all()


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_normalises_frequency_to_english() -> None:
    """BdE encodes frequency in Spanish (``MENSUAL``, ``TRIMESTRAL``); the
    enumerator translates so an agent searching ``monthly`` hits Spanish series."""
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    freqs = set(df["frequency"])
    assert "Monthly" in freqs
    assert "Quarterly" in freqs
    assert "Business Daily" in freqs


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_splits_dataset_from_leaf_title() -> None:
    """The ``/``-separated taxonomic path becomes ``dataset`` (METADATA) and
    the leaf becomes ``title`` (TITLE) — agents can filter by topic without
    coupling to the exact leaf phrasing."""
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    ti_row = df[df["key"] == "D_DTFK09A0"].iloc[0]
    # Title is the leaf segment after the last "/" in the upstream title path.
    assert ti_row["title"] == "Fixed rate auctions"
    # Dataset carries the joined ancestor segments, exposing the family path
    # (e.g. "Monetary policy › Eurosystem operations") as METADATA.
    assert "Monetary policy" in ti_row["dataset"]
    assert "Eurosystem operations" in ti_row["dataset"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_captures_dates_and_units_metadata() -> None:
    _mock_csv_chapters()
    df = (await enumerate_bde(BdeEnumerateParams())).data

    tc_row = df[df["key"] == "DTCCBCEUSDEUR.B"].iloc[0]
    assert tc_row["unit"].startswith("Dolares")
    assert tc_row["decimals"] == "4"
    assert tc_row["start_date"] == "04 ENE 1999"
    assert tc_row["end_date"] == "23 ABR 2026"
    assert tc_row["n_obs"] == "7124"
    assert tc_row["alias"] == "TC_1_1.1"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_degrades_gracefully_on_chapter_outage() -> None:
    """Per-chapter 5xx must not lose the surviving ones. Catalog completeness
    is best-effort; partial is strictly better than empty."""
    base = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
    # Four chapters succeed, three fail — verify we still get the surviving rows.
    respx.get(f"{base}/catalogo_be.csv").mock(
        return_value=httpx.Response(200, content=_CSV_BE.encode("cp1252"))
    )
    respx.get(f"{base}/catalogo_cf.csv").mock(return_value=httpx.Response(503))
    respx.get(f"{base}/catalogo_ie.csv").mock(
        return_value=httpx.Response(200, content=_CSV_IE.encode("cp1252"))
    )
    respx.get(f"{base}/catalogo_pb.csv").mock(return_value=httpx.Response(503))
    respx.get(f"{base}/catalogo_si.csv").mock(
        return_value=httpx.Response(200, content=_CSV_SI.encode("cp1252"))
    )
    respx.get(f"{base}/catalogo_tc.csv").mock(return_value=httpx.Response(500))
    respx.get(f"{base}/catalogo_ti.csv").mock(
        return_value=httpx.Response(200, content=_CSV_TI.encode("cp1252"))
    )

    df = (await enumerate_bde(BdeEnumerateParams())).data
    assert len(df) == 4
    assert set(df["category"]) == {
        "General Statistics",
        "International Economy",
        "Financial Indicators",
        "Interest Rates",
    }


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_skips_rows_missing_serie() -> None:
    """A row with no ``serie`` value can't be fetched and is dropped — this
    is a real failure mode in BdE's exporter when a description contains an
    unescaped quote."""
    base = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
    bogus = _csv(
        _HEADER,
        [
            # Missing serie — must be skipped without raising.
            _row(
                serie="",
                alias="TI_X.1",
                description="Phantom row",
                frequency_raw="MENSUAL",
                unit="%",
                decimals="2",
                start="ENE 2000",
                end="ENE 2026",
                nobs="312",
                title="Phantom",
                source_org="",
            ),
            # Real row — must be kept.
            _row(
                serie="D_DTFK09A0",
                alias="TI_1_1.1",
                description="Real row",
                frequency_raw="LABORABLE",
                unit="%",
                decimals="3",
                start="ENE 1999",
                end="ABR 2026",
                nobs="7125",
                title="Real",
                source_org="",
            ),
        ],
    )
    # Empty-body chapters must still produce header-only frames; respx returns
    # an empty body which the parser handles by falling through the row loop.
    # We give them a header-only CSV so empty-body decoding doesn't trip on
    # a missing header in the parser.
    header_only = _csv(_HEADER, [])
    for chapter in ("be", "cf", "ie", "pb", "si", "tc"):
        respx.get(f"{base}/catalogo_{chapter}.csv").mock(
            return_value=httpx.Response(200, content=header_only.encode("cp1252"))
        )
    respx.get(f"{base}/catalogo_ti.csv").mock(
        return_value=httpx.Response(200, content=bogus.encode("cp1252"))
    )

    df = (await enumerate_bde(BdeEnumerateParams())).data
    assert list(df["key"]) == ["D_DTFK09A0"]


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_returns_empty_when_all_chapters_fail() -> None:
    """All seven chapters down → empty frame with the declared schema columns,
    not a crash. Catalog publish jobs check ``len(df) == 0`` separately."""
    base = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
    for chapter in ("be", "cf", "ie", "pb", "si", "tc", "ti"):
        respx.get(f"{base}/catalogo_{chapter}.csv").mock(return_value=httpx.Response(503))

    df = (await enumerate_bde(BdeEnumerateParams())).data
    assert len(df) == 0
    # Schema columns are still present so downstream OutputConfig.apply works.
    assert "key" in df.columns
    assert "description" in df.columns
    assert "source" in df.columns


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_bde_emits_schema_with_description_role() -> None:
    """The OutputConfig declares a DESCRIPTION column so SeriesEntry.description
    is populated — and thus reaches the embedder via semantic_text()."""
    from parsimony.result import ColumnRole

    desc_cols = [c for c in BDE_ENUMERATE_OUTPUT.columns if c.role == ColumnRole.DESCRIPTION]
    assert len(desc_cols) == 1
    assert desc_cols[0].name == "description"


def test_enumerate_bde_schema_includes_source_metadata_column() -> None:
    """``source`` is a METADATA column on the schema (not just a frame column)
    so BM25 over the catalog can match on it."""
    from parsimony.result import ColumnRole

    meta_names = {c.name for c in BDE_ENUMERATE_OUTPUT.columns if c.role == ColumnRole.METADATA}
    assert "source" in meta_names
    assert {"frequency", "unit", "category", "start_date", "end_date", "decimals"} <= meta_names


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


def test_split_title_path_extracts_leaf_and_dataset() -> None:
    from parsimony_bde import _split_title_path

    dataset, leaf = _split_title_path("Monetary policy/Eurosystem operations/Fixed rate auctions")
    assert leaf == "Fixed rate auctions"
    assert "Monetary policy" in dataset
    assert "Eurosystem operations" in dataset


def test_split_title_path_handles_single_segment() -> None:
    from parsimony_bde import _split_title_path

    dataset, leaf = _split_title_path("Lone title")
    assert dataset == ""
    assert leaf == "Lone title"


def test_split_title_path_handles_empty() -> None:
    from parsimony_bde import _split_title_path

    dataset, leaf = _split_title_path("")
    assert dataset == ""
    assert leaf == ""


def test_split_title_path_treats_faceted_dsd_paths_as_dataset() -> None:
    """BdE's BE chapter encodes the title as ``Facet: value/Facet: value/...``
    where the last segment is just another facet, not a name. The leaf in that
    case is meaningless — the caller falls back to the description for the
    catalog title and keeps the whole faceted string as ``dataset``."""
    from parsimony_bde import _split_title_path

    dataset, leaf = _split_title_path(
        "Metodología: SEC2010/Año Base: 2020/Valoración: Volúmenes encadenados"
    )
    assert leaf == ""
    assert "Metodología" in dataset
    assert "Año Base" in dataset
    assert "Valoración" in dataset
