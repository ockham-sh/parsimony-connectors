"""Happy-path tests for the Banco de España connectors.

BdE BIEST is public (no api_key); template 401/429 contract does not apply.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest
import respx
from parsimony.catalog.source import entities_from_raw
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import Result

from parsimony_bde import CONNECTORS
from parsimony_bde._http import PB_ZIP_URL
from parsimony_bde.connectors.enumerate import enumerate_bde
from parsimony_bde.connectors.fetch import bde_fetch
from parsimony_bde.outputs import BDE_ENUMERATE_OUTPUT

_LISTA_SERIES_URL = "https://app.bde.es/bierest/resources/srdatosapp/listaSeries"
_CSV_BASE = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/csv"
_CSV_CHAPTERS = ("be", "cf", "ie", "si", "tc", "ti")  # pb is recovered from pb.zip

_ENUMERATE_FRAME_COLUMNS = [
    "key",
    "title",
    "description",
    "source",
    "alias",
    "dataset",
    "category",
    "frequency",
    "unit",
    "decimals",
    "start_date",
    "end_date",
    "n_obs",
    "source_org",
]


def _enumerate_frame(result: Result) -> pd.DataFrame:
    """Project enumerator tabular output into a flat frame for assertions."""
    entries = entities_from_raw(result.data, BDE_ENUMERATE_OUTPUT)
    if not entries:
        return pd.DataFrame(columns=_ENUMERATE_FRAME_COLUMNS)
    rows = [{"key": entry.code, "title": entry.title, **entry.metadata} for entry in entries]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"bde_fetch", "enumerate_bde", "bde_search"}


# ---------------------------------------------------------------------------
# bde_fetch
# ---------------------------------------------------------------------------


def _series_record(
    serie: str = "D_1NBAF472",
    *,
    short_desc: str = "One-year Euribor",
    dates: list[str] | None = None,
    values: list[float | str] | None = None,
) -> dict[str, object]:
    """A minimal BdE ``listaSeries`` record matching the live JSON shape."""
    return {
        "serie": serie,
        "descripcion": short_desc,
        "descripcionCorta": short_desc,
        "codFrecuencia": "M",
        "decimales": 3,
        "fechas": dates if dates is not None else ["2026-01-01T08:15:00Z", "2026-02-01T08:15:00Z"],
        "valores": values if values is not None else [2.804, 2.747],
    }


@respx.mock
def test_bde_fetch_parses_single_series_response() -> None:
    respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(200, json=[_series_record()]))

    result = bde_fetch(key="D_1NBAF472")

    assert result.provenance.source == "bde_fetch"
    # Secrets/keyless: provenance records the call-time args verbatim.
    assert result.provenance.params == {"key": "D_1NBAF472", "time_range": None, "lang": "en"}
    df = result.data
    assert list(df["key"].unique()) == ["D_1NBAF472"]
    assert df["title"].iloc[0] == "One-year Euribor"
    # The ISO timestamp is parsed to a real datetime by the connector body.
    assert df["date"].dtype.kind == "M"
    assert df["date"].iloc[0] == pd.Timestamp("2026-01-01")
    # The body parses values to floats.
    assert df["value"].dtype.kind == "f"
    assert df["value"].iloc[0] == pytest.approx(2.804)


@respx.mock
def test_bde_fetch_sorts_observations_ascending_by_date() -> None:
    # BdE returns rows newest-first; the connector sorts ascending so downstream
    # joins don't need to re-sort.
    respx.get(_LISTA_SERIES_URL).mock(
        return_value=httpx.Response(
            200,
            json=[
                _series_record(
                    dates=["2026-03-01T08:15:00Z", "2026-01-01T08:15:00Z", "2026-02-01T08:15:00Z"],
                    values=[3.0, 1.0, 2.0],
                )
            ],
        )
    )

    df = bde_fetch(key="D_1NBAF472").data

    assert list(df["value"]) == [1.0, 2.0, 3.0]
    assert list(df["date"]) == [
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-02-01"),
        pd.Timestamp("2026-03-01"),
    ]


@respx.mock
def test_bde_fetch_multi_series_kept_contiguous_when_sorting() -> None:
    # A multi-series request sorts by (key, date): each series stays contiguous
    # and ascending, rather than interleaving the two series by date.
    respx.get(_LISTA_SERIES_URL).mock(
        return_value=httpx.Response(
            200,
            json=[
                _series_record("BBB", dates=["2026-02-01T00:00:00Z", "2026-01-01T00:00:00Z"], values=[2.0, 1.0]),
                _series_record("AAA", dates=["2026-02-01T00:00:00Z", "2026-01-01T00:00:00Z"], values=[20.0, 10.0]),
            ],
        )
    )

    df = bde_fetch(key="AAA,BBB").data

    assert list(df["key"]) == ["AAA", "AAA", "BBB", "BBB"]
    assert list(df["value"]) == [10.0, 20.0, 1.0, 2.0]


@respx.mock
def test_bde_fetch_sends_one_request_for_comma_joined_keys() -> None:
    """Multiple comma-separated codes go out in a SINGLE request (BdE supports
    a comma-joined ``series`` param) — not one request per key."""
    route = respx.get(_LISTA_SERIES_URL).mock(
        return_value=httpx.Response(
            200,
            json=[_series_record("D_1NBAF472"), _series_record("DTCCBCEUSDEUR.B", short_desc="USD/EUR")],
        )
    )

    df = (bde_fetch(key="D_1NBAF472, DTCCBCEUSDEUR.B")).data

    assert len(route.calls) == 1
    sent = route.calls.last.request
    assert sent.url.params["series"] == "D_1NBAF472,DTCCBCEUSDEUR.B"
    assert set(df["key"]) == {"D_1NBAF472", "DTCCBCEUSDEUR.B"}


@respx.mock
def test_bde_fetch_passes_time_range_and_lang() -> None:
    route = respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(200, json=[_series_record()]))

    bde_fetch(key="D_1NBAF472", time_range="30m", lang="es")

    sent = route.calls.last.request
    assert sent.url.params["idioma"] == "es"
    # Lowercase keyword is normalised to canonical uppercase.
    assert sent.url.params["rango"] == "30M"


@respx.mock
def test_bde_fetch_omits_rango_when_no_time_range() -> None:
    route = respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(200, json=[_series_record()]))

    bde_fetch(key="D_1NBAF472")

    # fetch_json drops None-valued params, so no full-range default leaks out.
    assert "rango" not in route.calls.last.request.url.params


@respx.mock
def test_bde_fetch_raises_empty_data_on_empty_list() -> None:
    respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(200, json=[]))

    with pytest.raises(EmptyDataError) as exc:
        bde_fetch(key="XX")
    assert exc.value.query_params == {"key": "XX", "time_range": None, "lang": "en"}


@respx.mock
def test_bde_fetch_raises_empty_data_when_series_has_no_observations() -> None:
    respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(200, json=[_series_record(dates=[], values=[])]))

    with pytest.raises(EmptyDataError):
        bde_fetch(key="D_1NBAF472")


@respx.mock
def test_bde_fetch_raises_parse_error_on_non_list_shape() -> None:
    # HTTP 200 but a JSON object, not the expected list -> ParseError (§5.8).
    respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(200, json={"errNum": 412}))

    with pytest.raises(ParseError):
        bde_fetch(key="D_1NBAF472")


@respx.mock
def test_bde_fetch_maps_412_to_invalid_parameter_with_bde_detail() -> None:
    # BdE answers an unknown series / frequency-incompatible range with HTTP 412
    # and an errMsgDebug body. That's a caller-input error → InvalidParameterError
    # carrying BdE's own message (not a generic ProviderError the caller would
    # mistake for a transient server fault).
    respx.get(_LISTA_SERIES_URL).mock(
        return_value=httpx.Response(
            412,
            json={
                "errNum": 412,
                "errMsgUsr": "Error de validación en la solicitud",
                "errMsgDebug": "La serie NOPE no existe",
            },
        )
    )

    with pytest.raises(InvalidParameterError) as exc:
        bde_fetch(key="NOPE")
    assert "no existe" in str(exc.value)


@respx.mock
def test_bde_fetch_maps_non_412_http_error_to_provider_error() -> None:
    from parsimony.errors import ProviderError

    # A genuine server fault (5xx) stays a ProviderError via the canonical mapping.
    respx.get(_LISTA_SERIES_URL).mock(return_value=httpx.Response(503))

    with pytest.raises(ProviderError) as exc:
        bde_fetch(key="D_1NBAF472")
    assert exc.value.status_code == 503


@respx.mock
def test_bde_fetch_accepts_daily_range_codes() -> None:
    """Daily series take 3M/12M/36M (not MAX). The client must NOT reject these
    as it did when the valid set was hardcoded to {30M,60M,MAX}."""
    route = respx.get(_LISTA_SERIES_URL).mock(
        return_value=httpx.Response(200, json=[_series_record("DTCCBCEUSDEUR.B", short_desc="USD/EUR")])
    )

    bde_fetch(key="DTCCBCEUSDEUR.B", time_range="12m")

    assert route.calls.last.request.url.params["rango"] == "12M"


def test_bde_fetch_rejects_malformed_time_range() -> None:
    # A value that is neither a known range code nor a 4-digit year is rejected
    # client-side before any network call.
    with pytest.raises(InvalidParameterError, match="time_range"):
        bde_fetch(key="D_1NBAF472", time_range="5Y")


def test_bde_fetch_rejects_empty_key() -> None:
    with pytest.raises(InvalidParameterError):
        bde_fetch(key="  ")


def test_bde_fetch_rejects_unknown_lang() -> None:
    with pytest.raises(InvalidParameterError):
        bde_fetch(key="D_1NBAF472", lang="fr")


def test_bde_fetch_namespace_hint() -> None:
    assert dict(bde_fetch.namespace_hints) == {"key": "bde"}


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
# ``pb`` (Bank Lending Survey) is recovered from the bulk ``pb.zip``, whose
# value files are TRANSPOSED: row 0 is the real fetchable ``DPB…`` codes, row 2
# the ``PB_x_y.z`` aliases, then description/units/frequency rows, then one row
# per observation date. The catalog CSV lists only the un-fetchable aliases, so
# the connector reads the ZIP instead.
def _pb_member_csv(series: list[dict[str, str]]) -> str:
    """Render one transposed ``pb_*.csv`` member the way BdE's exporter does."""
    import csv as _csv_mod
    import io

    n = len(series)
    rows: list[list[str]] = [
        ["NOMBRE DE LA SERIE", *[s["serie"] for s in series]],
        ["NÚMERO SECUENCIAL", *[str(1815300 + i) for i in range(n)]],
        ["ALIAS DE LA SERIE", *[s["alias"] for s in series]],
        ["DESCRIPCIÓN DE LA SERIE", *[s["description"] for s in series]],
        ["DESCRIPCIÓN DE LAS UNIDADES", *[s.get("unit", "Porcentaje") for s in series]],
        ["FRECUENCIA", *[s.get("frequency_raw", "TRIMESTRAL") for s in series]],
    ]
    # Two observation dates; a column with "" at a date means "no obs there".
    for date_label in ("DIC 2002", "MAR 2003"):
        rows.append([date_label, *[s.get(date_label, "10.0") for s in series]])
    buf = io.StringIO()
    writer = _csv_mod.writer(buf, quoting=_csv_mod.QUOTE_MINIMAL)
    for r in rows:
        writer.writerow(r)
    return buf.getvalue()


def _pb_zip(members: dict[str, str]) -> bytes:
    """Build an in-memory ``pb.zip`` from {member_name: csv_text}."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, text in members.items():
            zf.writestr(name, text.encode("cp1252"))
    return buf.getvalue()


_PB_ZIP_DEFAULT = _pb_zip(
    {
        "pb_1_1.csv": _pb_member_csv(
            [
                {
                    "serie": "DPBCOCCNFNOAPTOPN.T.ES",
                    "alias": "PB_1_1.1",
                    "description": "EPB. %neto. Cambios criterios aprobación préstamos a SNF.",
                },
                {
                    "serie": "DPBPOCCNFNOAPTOPN.T.ES",
                    "alias": "PB_1_1.2",
                    "description": "EPB. %neto. Previsión criterios aprobación préstamos a SNF.",
                },
            ]
        ),
    }
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
            description=("Economía internacional. Índice de precios internacionales de materias primas en euros."),
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


def _mock_csv_chapters(*, pb_zip: bytes | None = _PB_ZIP_DEFAULT) -> None:
    payloads = {
        "be": _CSV_BE,
        "cf": _CSV_CF,
        "ie": _CSV_IE,
        "si": _CSV_SI,
        "tc": _CSV_TC,
        "ti": _CSV_TI,
    }
    for chapter, body in payloads.items():
        respx.get(f"{_CSV_BASE}/catalogo_{chapter}.csv").mock(
            return_value=httpx.Response(
                200,
                # Encode in CP1252 so the production decoder path is exercised.
                content=body.encode("cp1252"),
                headers={"content-type": "text/csv"},
            )
        )
    if pb_zip is not None:
        respx.get(PB_ZIP_URL).mock(
            return_value=httpx.Response(200, content=pb_zip, headers={"content-type": "application/zip"})
        )


@respx.mock
def test_enumerate_bde_pulls_all_seven_catalog_sources() -> None:
    _mock_csv_chapters()
    result = enumerate_bde()
    df = _enumerate_frame(result)

    # Six CSV chapters contribute one row each; the seventh source — the Bank
    # Lending Survey — is recovered from pb.zip and contributes its two series.
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
    assert len(df) == 8  # 6 CSV rows + 2 recovered BLS series


@respx.mock
def test_enumerate_bde_recovers_bank_lending_survey_from_zip() -> None:
    """The pb CSV lists un-fetchable family aliases (PB_1_1.1); the real
    fetchable codes (DPB…) live only in pb.zip. Enumerate must surface the real
    codes so a search hit can actually be fetched by bde_fetch."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    pb_rows = df[df["category"] == "Bank Lending Survey"]
    assert set(pb_rows["key"]) == {"DPBCOCCNFNOAPTOPN.T.ES", "DPBPOCCNFNOAPTOPN.T.ES"}
    # The un-fetchable alias is preserved as METADATA, not as the fetch key.
    first = pb_rows[pb_rows["key"] == "DPBCOCCNFNOAPTOPN.T.ES"].iloc[0]
    assert first["alias"] == "PB_1_1.1"
    assert first["frequency"] == "Quarterly"
    assert first["n_obs"] == "2"  # two observation rows in the fixture
    # No un-fetchable PB_x_y.z code leaks into the catalog keys.
    assert not df["key"].str.startswith("PB_").any()


@respx.mock
def test_enumerate_bde_dedupes_cross_chapter_repeats() -> None:
    """A series listed under two thematic chapters must appear once. First
    occurrence in chapter order wins, so the result is deterministic."""
    dup = _csv(
        _HEADER,
        [
            _row(
                serie="D_DUP001",
                alias="BE_9.1",
                description="Shared series (home chapter).",
                frequency_raw="MENSUAL",
                unit="%",
                decimals="2",
                start="ENE 2000",
                end="ENE 2026",
                nobs="312",
                title="Home/Shared",
                source_org="",
            ),
        ],
    )
    dup_si = _csv(
        _HEADER,
        [
            _row(
                serie="D_DUP001",
                alias="SI_9.1",
                description="Shared series (summary chapter).",
                frequency_raw="MENSUAL",
                unit="%",
                decimals="2",
                start="ENE 2000",
                end="ENE 2026",
                nobs="312",
                title="Summary/Shared",
                source_org="",
            ),
        ],
    )
    for chapter in ("cf", "ie", "tc", "ti"):
        respx.get(f"{_CSV_BASE}/catalogo_{chapter}.csv").mock(
            return_value=httpx.Response(200, content=_csv(_HEADER, []).encode("cp1252"))
        )
    respx.get(f"{_CSV_BASE}/catalogo_be.csv").mock(
        return_value=httpx.Response(200, content=dup.encode("cp1252"))
    )
    respx.get(f"{_CSV_BASE}/catalogo_si.csv").mock(
        return_value=httpx.Response(200, content=dup_si.encode("cp1252"))
    )
    respx.get(PB_ZIP_URL).mock(return_value=httpx.Response(503))

    df = _enumerate_frame(enumerate_bde())
    assert list(df["key"]) == ["D_DUP001"]
    # ``be`` precedes ``si`` in chapter order, so the home-chapter row wins.
    assert df.iloc[0]["category"] == "General Statistics"


@respx.mock
def test_enumerate_bde_includes_cf_financial_accounts_rows() -> None:
    """CF chapter (CFEE / Financial Accounts of the Spanish Economy) ships
    the bulk of SEC2010 sector balance-sheet series — ~4.7k rows live here
    and nowhere else. Regression guard: if someone removes ``cf`` from
    ``_CATALOG_CHAPTERS`` we lose those series silently."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    cf_rows = df[df["category"] == "Financial Accounts"]
    assert len(cf_rows) >= 1
    cf_row = cf_rows.iloc[0]
    # CF titles are faceted DSD paths; leaf is empty so title falls back to
    # the description (the only user-facing prose we have).
    assert "CFEE" in cf_row["title"] or "SEC2010" in cf_row["title"]
    # Faceted path lands in ``dataset`` METADATA, not ``title``.
    assert "SEC2010" in cf_row["dataset"]


@respx.mock
def test_enumerate_bde_includes_ie_international_economy_rows() -> None:
    """IE chapter (International Economy) carries world-price and
    commodity-index series. Most IE codes overlap with BE under a
    different taxonomy, but the category-filtered view still matters."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    ie_rows = df[df["category"] == "International Economy"]
    assert len(ie_rows) >= 1
    assert "ECONOMIA MUNDIAL" in ie_rows.iloc[0]["dataset"]


@respx.mock
def test_enumerate_bde_populates_description_column() -> None:
    """The description metadata carries upstream long-form prose for search."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    ti_row = df[df["key"] == "D_DTFK09A0"].iloc[0]
    # Spanish character round-trips through CP1252 → str unscathed.
    assert "Tipo de interés" in ti_row["description"]
    # No description column row is empty — every series ships with one.
    assert (df["description"].str.len() > 0).all()


@respx.mock
def test_enumerate_bde_carries_source_metadata_for_dispatch() -> None:
    """Every catalog row carries ``source`` so an agent dispatching off a
    search hit knows which fetch connector to call without parsing the key."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    assert "source" in df.columns
    assert (df["source"] == "bde_biest").all()


@respx.mock
def test_enumerate_bde_normalises_frequency_to_english() -> None:
    """BdE encodes frequency in Spanish (``MENSUAL``, ``TRIMESTRAL``); the
    enumerator translates so an agent searching ``monthly`` hits Spanish series."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    freqs = set(df["frequency"])
    assert "Monthly" in freqs
    assert "Quarterly" in freqs
    assert "Business Daily" in freqs


@respx.mock
def test_enumerate_bde_splits_dataset_from_leaf_title() -> None:
    """The ``/``-separated taxonomic path becomes ``dataset`` (METADATA) and
    the leaf becomes ``title`` (TITLE) — agents can filter by topic without
    coupling to the exact leaf phrasing."""
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    ti_row = df[df["key"] == "D_DTFK09A0"].iloc[0]
    # Title is the leaf segment after the last "/" in the upstream title path.
    assert ti_row["title"] == "Fixed rate auctions"
    # Dataset carries the joined ancestor segments, exposing the family path
    # (e.g. "Monetary policy › Eurosystem operations") as METADATA.
    assert "Monetary policy" in ti_row["dataset"]
    assert "Eurosystem operations" in ti_row["dataset"]


@respx.mock
def test_enumerate_bde_captures_dates_and_units_metadata() -> None:
    _mock_csv_chapters()
    df = _enumerate_frame(enumerate_bde())

    tc_row = df[df["key"] == "DTCCBCEUSDEUR.B"].iloc[0]
    assert tc_row["unit"].startswith("Dolares")
    assert tc_row["decimals"] == "4"
    assert tc_row["start_date"] == "04 ENE 1999"
    assert tc_row["end_date"] == "23 ABR 2026"
    assert tc_row["n_obs"] == "7124"
    assert tc_row["alias"] == "TC_1_1.1"


@respx.mock
def test_enumerate_bde_degrades_gracefully_on_chapter_outage() -> None:
    """Per-chapter 5xx must not lose the surviving ones. Catalog completeness
    is best-effort; partial is strictly better than empty."""
    # Three CSV chapters succeed; the rest (and pb.zip) fail — verify we still
    # get the surviving rows rather than losing everything.
    respx.get(f"{_CSV_BASE}/catalogo_be.csv").mock(
        return_value=httpx.Response(200, content=_CSV_BE.encode("cp1252"))
    )
    respx.get(f"{_CSV_BASE}/catalogo_cf.csv").mock(return_value=httpx.Response(503))
    respx.get(f"{_CSV_BASE}/catalogo_ie.csv").mock(
        return_value=httpx.Response(200, content=_CSV_IE.encode("cp1252"))
    )
    respx.get(f"{_CSV_BASE}/catalogo_si.csv").mock(
        return_value=httpx.Response(200, content=_CSV_SI.encode("cp1252"))
    )
    respx.get(f"{_CSV_BASE}/catalogo_tc.csv").mock(return_value=httpx.Response(500))
    respx.get(f"{_CSV_BASE}/catalogo_ti.csv").mock(return_value=httpx.Response(503))
    respx.get(PB_ZIP_URL).mock(return_value=httpx.Response(503))

    df = _enumerate_frame(enumerate_bde())
    assert len(df) == 3
    assert set(df["category"]) == {
        "General Statistics",
        "International Economy",
        "Financial Indicators",
    }


@respx.mock
def test_enumerate_bde_skips_rows_missing_serie() -> None:
    """A row with no ``serie`` value can't be fetched and is dropped — this
    is a real failure mode in BdE's exporter when a description contains an
    unescaped quote."""
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
    for chapter in ("be", "cf", "ie", "si", "tc"):
        respx.get(f"{_CSV_BASE}/catalogo_{chapter}.csv").mock(
            return_value=httpx.Response(200, content=header_only.encode("cp1252"))
        )
    respx.get(f"{_CSV_BASE}/catalogo_ti.csv").mock(
        return_value=httpx.Response(200, content=bogus.encode("cp1252"))
    )
    respx.get(PB_ZIP_URL).mock(return_value=httpx.Response(503))

    df = _enumerate_frame(enumerate_bde())
    assert list(df["key"]) == ["D_DTFK09A0"]


@respx.mock
def test_enumerate_bde_returns_empty_when_all_chapters_fail() -> None:
    """All sources down (6 CSV chapters + pb.zip) → empty frame with the declared
    schema columns, not a crash. Catalog publish jobs check ``len(df) == 0``."""
    for chapter in _CSV_CHAPTERS:
        respx.get(f"{_CSV_BASE}/catalogo_{chapter}.csv").mock(return_value=httpx.Response(503))
    respx.get(PB_ZIP_URL).mock(return_value=httpx.Response(503))

    df = _enumerate_frame(enumerate_bde())
    assert len(df) == 0
    # Schema columns are still present so downstream OutputSpec.apply works.
    assert "key" in df.columns
    assert "description" in df.columns
    assert "source" in df.columns


@respx.mock
def test_enumerate_bde_emits_description_as_metadata() -> None:
    """Descriptions are ordinary metadata in the clean catalog contract."""
    from parsimony.result import ColumnRole

    desc_cols = [c for c in BDE_ENUMERATE_OUTPUT.columns if c.name == "description"]
    assert len(desc_cols) == 1
    assert desc_cols[0].role == ColumnRole.METADATA


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
    from parsimony_bde.connectors._catalog import split_title_path

    dataset, leaf = split_title_path("Monetary policy/Eurosystem operations/Fixed rate auctions")
    assert leaf == "Fixed rate auctions"
    assert "Monetary policy" in dataset
    assert "Eurosystem operations" in dataset


def test_split_title_path_handles_single_segment() -> None:
    from parsimony_bde.connectors._catalog import split_title_path

    dataset, leaf = split_title_path("Lone title")
    assert dataset == ""
    assert leaf == "Lone title"


def test_split_title_path_handles_empty() -> None:
    from parsimony_bde.connectors._catalog import split_title_path

    dataset, leaf = split_title_path("")
    assert dataset == ""
    assert leaf == ""


def test_split_title_path_treats_faceted_dsd_paths_as_dataset() -> None:
    """BdE's BE chapter encodes the title as ``Facet: value/Facet: value/...``
    where the last segment is just another facet, not a name. The leaf in that
    case is meaningless — the caller falls back to the description for the
    catalog title and keeps the whole faceted string as ``dataset``."""
    from parsimony_bde.connectors._catalog import split_title_path

    dataset, leaf = split_title_path("Metodología: SEC2010/Año Base: 2020/Valoración: Volúmenes encadenados")
    assert leaf == ""
    assert "Metodología" in dataset
    assert "Año Base" in dataset
    assert "Valoración" in dataset
