"""Offline (respx-mocked) tests for the Riksbank connectors.

All five Riksbank products are open / keyless for fetch and enumeration; the
``Ocp-Apim-Subscription-Key`` header is optional and only raises the quota. The
connectors default ``api_key=""`` and skip the header when empty, so there is no
``UnauthorizedError`` fast-fail on the fetch/enumerate happy path.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.result import ColumnRole

from parsimony_riksbank import (
    CONNECTORS,
    RIKSBANK_ENUMERATE_OUTPUT,
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_holdings_fetch,
    riksbank_monetary_policy_fetch,
    riksbank_swestr_fetch,
    riksbank_turnover_fetch,
)

_SWEA = "https://api.riksbank.se/swea/v1"
_SWESTR = "https://api.riksbank.se/swestr/v1"
_MP = "https://api.riksbank.se/monetary_policy_data/v1/forecasts"
_TURN = "https://api.riksbank.se/turnover-statistics/v1"
_HOLD = "https://api.riksbank.se/holdings/v1"


# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {
        "riksbank_fetch",
        "riksbank_swestr_fetch",
        "riksbank_monetary_policy_fetch",
        "riksbank_turnover_fetch",
        "riksbank_holdings_fetch",
        "enumerate_riksbank",
        "riksbank_search",
    }


def test_all_keyed_verbs_declare_api_key_secret() -> None:
    """The optional ``api_key`` is stripped from provenance on every keyed verb."""
    for name in (
        "riksbank_fetch",
        "riksbank_swestr_fetch",
        "riksbank_monetary_policy_fetch",
        "riksbank_turnover_fetch",
        "riksbank_holdings_fetch",
        "enumerate_riksbank",
    ):
        conn = CONNECTORS[name]
        assert "api_key" in conn.secrets, f"{name} must declare api_key in secrets="


def test_enumerate_output_declares_code_description_and_source_columns() -> None:
    """The enumerator KEY is the routable ``code``; ``description`` carries searchable
    prose and ``source`` routes a hit to the right fetch verb."""
    by_role: dict[ColumnRole, list[str]] = {}
    for col in RIKSBANK_ENUMERATE_OUTPUT.columns:
        by_role.setdefault(col.role, []).append(col.name)
    assert by_role[ColumnRole.KEY] == ["code"]
    assert "description" in by_role[ColumnRole.METADATA]
    assert "source" in by_role[ColumnRole.METADATA]


# ---------------------------------------------------------------------------
# riksbank_fetch (SWEA)
# ---------------------------------------------------------------------------


@respx.mock
def test_riksbank_fetch_latest_single_object() -> None:
    """SWEA ``/Observations/Latest/{id}`` returns a single JSON object (not
    a list). The connector wraps it into a one-row DataFrame."""
    respx.get(f"{_SWEA}/Observations/Latest/SEKEURPMI").mock(
        return_value=httpx.Response(200, json={"date": "2026-06-03", "value": 10.884})
    )
    respx.get(f"{_SWEA}/Series").mock(
        return_value=httpx.Response(
            200, json=[{"seriesId": "SEKEURPMI", "shortDescription": "EUR", "source": "Refinitiv"}]
        )
    )

    result = riksbank_fetch(series_id="SEKEURPMI")

    assert result.provenance.source == "riksbank_fetch"
    df = result.raw
    assert len(df) == 1
    assert df.iloc[0]["title"] == "EUR"
    assert df.iloc[0]["value"] == pytest.approx(10.884)


@respx.mock
def test_riksbank_fetch_window_returns_list() -> None:
    respx.get(f"{_SWEA}/Observations/SEKEURPMI/2026-01-01/2026-01-10").mock(
        return_value=httpx.Response(
            200, json=[{"date": "2026-01-02", "value": 10.8085}, {"date": "2026-01-05", "value": 10.787}]
        )
    )
    respx.get(f"{_SWEA}/Series").mock(
        return_value=httpx.Response(200, json=[{"seriesId": "SEKEURPMI", "shortDescription": "EUR"}])
    )

    result = riksbank_fetch(series_id="SEKEURPMI", from_date="2026-01-01", to_date="2026-01-10")
    df = result.raw
    assert len(df) == 2
    assert list(df["date"].dt.strftime("%Y-%m-%d")) == ["2026-01-02", "2026-01-05"]


@respx.mock
def test_riksbank_fetch_title_lookup_failure_falls_back_to_id() -> None:
    """A transient operational failure of the secondary /Series title lookup
    must NOT fail the whole fetch — it falls back to the series id. (Replaces
    the old bare ``except Exception: pass`` swallow.)"""
    respx.get(f"{_SWEA}/Observations/Latest/SEKEURPMI").mock(
        return_value=httpx.Response(200, json={"date": "2026-06-03", "value": 10.884})
    )
    # /Series 429s — a typed ConnectorError the lookup tolerates.
    respx.get(f"{_SWEA}/Series").mock(return_value=httpx.Response(429, text="rate limited"))

    result = riksbank_fetch(series_id="SEKEURPMI")
    df = result.raw
    assert len(df) == 1
    # Falls back to the id rather than raising or silently swallowing.
    assert df.iloc[0]["title"] == "SEKEURPMI"


@respx.mock
def test_riksbank_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get(f"{_SWEA}/Observations/Latest/XX").mock(return_value=httpx.Response(200, json=[]))
    respx.get(f"{_SWEA}/Series").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        riksbank_fetch(series_id="XX")


def test_fetch_rejects_empty_series_id() -> None:
    """Inline validation replaces the deleted RiksbankFetchParams model."""

    def _run() -> None:
        riksbank_fetch(series_id="   ")

    with pytest.raises(InvalidParameterError):
        _run()


def test_fetch_rejects_lonely_from_date() -> None:
    """``from_date`` without ``to_date`` is ambiguous against the window-vs-
    latest dispatch. The (now-correct) date-pair validator rejects it. This
    is the bug the old ``_both_dates_or_neither`` @field_validator missed
    because it lacked ``validate_default=True`` — now an inline guard that
    always fires."""
    with pytest.raises(InvalidParameterError):
        riksbank_fetch(series_id="SEKEURPMI", from_date="2026-01-01")


def test_fetch_rejects_lonely_to_date() -> None:
    with pytest.raises(InvalidParameterError):
        riksbank_fetch(series_id="SEKEURPMI", to_date="2026-01-10")


# ---------------------------------------------------------------------------
# riksbank_swestr_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_riksbank_swestr_fetch_latest_rate_hits_latest_endpoint() -> None:
    """With no date window, the raw SWESTR fixing goes to ``/latest/SWESTR``
    and returns a single-row DataFrame with the published rate."""
    respx.get(f"{_SWESTR}/latest/SWESTR").mock(
        return_value=httpx.Response(
            200,
            json={"rate": 1.639, "date": "2026-04-23", "numberOfTransactions": 255, "volume": 74459},
        )
    )
    result = riksbank_swestr_fetch(series="SWESTR")
    df = result.raw
    assert len(df) == 1
    assert df.iloc[0]["value"] == pytest.approx(1.639)
    assert df.iloc[0]["series"] == "SWESTR"
    assert df.iloc[0]["title"] == "SWESTR — Swedish Krona Short-Term Rate"
    assert df.iloc[0]["numberOfTransactions"] == 255


@respx.mock
def test_riksbank_swestr_fetch_windowed_average_hits_avg_endpoint() -> None:
    """A compounded average with a date window routes to ``/avg/<id>``."""
    respx.get(f"{_SWESTR}/avg/SWESTRAVG1W").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"rate": 1.633, "date": "2026-04-15", "startDate": "2026-04-08"},
                {"rate": 1.636, "date": "2026-04-16", "startDate": "2026-04-09"},
            ],
        )
    )
    result = riksbank_swestr_fetch(series="SWESTRAVG1W", from_date="2026-04-15", to_date="2026-04-16")
    df = result.raw
    assert len(df) == 2
    assert list(df["series"].unique()) == ["SWESTRAVG1W"]
    assert df.iloc[0]["startDate"] == "2026-04-08"


@respx.mock
def test_riksbank_swestr_fetch_index_normalises_value_field() -> None:
    """The SWESTR index publishes ``value`` (an index level) rather than
    ``rate``; the connector normalises both onto a single ``value`` column."""
    respx.get(f"{_SWESTR}/index/latest/SWESTRINDEX").mock(
        return_value=httpx.Response(
            200,
            json={"value": 110.25032277, "date": "2026-04-24", "republication": False},
        )
    )
    result = riksbank_swestr_fetch(series="SWESTRINDEX")
    df = result.raw
    assert len(df) == 1
    assert df.iloc[0]["value"] == pytest.approx(110.25032277)


@respx.mock
def test_riksbank_swestr_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get(f"{_SWESTR}/latest/SWESTR").mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(EmptyDataError):
        riksbank_swestr_fetch(series="SWESTR")


def test_swestr_fetch_rejects_lonely_from_date() -> None:
    """``from_date`` without ``to_date`` → InvalidParameterError (inline)."""
    with pytest.raises(InvalidParameterError):
        riksbank_swestr_fetch(series="SWESTR", from_date="2026-01-01")


# ---------------------------------------------------------------------------
# riksbank_monetary_policy_fetch
# ---------------------------------------------------------------------------

_MP_ONE_ROUND = {
    "data": [
        {
            "external_id": "SEQGDPNAYSA",
            "metadata": {"description": "GDP", "unit": "Annual percentage change"},
            "vintages": {
                "metadata": {"forecast_cutoff_date": "2025-12-31", "policy_round": "2026:1"},
                "observations": [{"dt": "2025-12-31", "value": 1.2}, {"dt": "2026-03-31", "value": 1.5}],
            },
        }
    ]
}

_MP_ALL_VINTAGES = {
    "data": [
        {
            "external_id": "SEQGDPNAYSA",
            "metadata": {"description": "GDP", "unit": "Annual percentage change"},
            "vintages": [
                {
                    "metadata": {"policy_round": "2026:1", "forecast_cutoff_date": "2025-12-31"},
                    "observations": [{"dt": "2026-03-31", "value": 1.5}],
                },
                {
                    "metadata": {"policy_round": "2025:5", "forecast_cutoff_date": "2025-10-31"},
                    "observations": [{"dt": "2026-03-31", "value": 1.1}],
                },
            ],
        }
    ]
}


@respx.mock
def test_monetary_policy_fetch_single_round() -> None:
    respx.get(url__regex=rf"{_MP}.*").mock(return_value=httpx.Response(200, json=_MP_ONE_ROUND))
    result = riksbank_monetary_policy_fetch(series="SEQGDPNAYSA", policy_round="2026:1")
    assert result.provenance.source == "riksbank_monetary_policy_fetch"
    df = result.raw
    assert len(df) == 2
    assert set(df["series"]) == {"SEQGDPNAYSA"}
    assert set(df["policy_round"]) == {"2026:1"}
    assert df.iloc[0]["forecast_cutoff_date"] == "2025-12-31"
    assert df.iloc[0]["title"] == "GDP (Annual percentage change)"
    assert df["value"].dtype.kind == "f"
    assert df["date"].dtype.kind == "M"


@respx.mock
def test_monetary_policy_fetch_all_vintages_disambiguated_by_round() -> None:
    """Omitting the round returns a *list* of vintages; the policy_round column keeps them apart."""
    respx.get(url__regex=rf"{_MP}.*").mock(return_value=httpx.Response(200, json=_MP_ALL_VINTAGES))
    result = riksbank_monetary_policy_fetch(series="SEQGDPNAYSA")
    df = result.raw
    assert set(df["policy_round"]) == {"2026:1", "2025:5"}
    assert len(df) == 2


@respx.mock
def test_monetary_policy_fetch_empty_raises() -> None:
    respx.get(url__regex=rf"{_MP}.*").mock(return_value=httpx.Response(200, json={"data": []}))
    with pytest.raises(EmptyDataError):
        riksbank_monetary_policy_fetch(series="NOPE")


def test_monetary_policy_fetch_rejects_empty_series() -> None:
    with pytest.raises(InvalidParameterError):
        riksbank_monetary_policy_fetch(series="  ")


# ---------------------------------------------------------------------------
# riksbank_turnover_fetch
# ---------------------------------------------------------------------------

_TURN_PAYLOAD = [
    {"Period": "2025-05-01", "Asset": "SEKEUR", "Contract": "SP", "Counterparty": "REP", "Amount": 10199},
    {"Period": "2025-05-01", "Asset": "SEKEUR", "Contract": "FO", "Counterparty": "OMM", "Amount": 1567},
]


@respx.mock
def test_turnover_fetch_parses_faceted_records() -> None:
    respx.get(f"{_TURN}/markets/fx/frequencies/monthly").mock(return_value=httpx.Response(200, json=_TURN_PAYLOAD))
    result = riksbank_turnover_fetch(market="fx", frequency="monthly")
    assert result.provenance.source == "riksbank_turnover_fetch"
    df = result.raw
    assert len(df) == 2
    assert set(df["market"]) == {"fx"}
    assert df["period"].dtype.kind == "M"
    assert df["amount"].dtype.kind == "f"
    assert set(df["contract"]) == {"SP", "FO"}
    assert {"asset", "counterparty"} <= set(df.columns)


def test_turnover_fetch_rejects_bad_market() -> None:
    with pytest.raises(InvalidParameterError):
        riksbank_turnover_fetch(market="equities", frequency="monthly")  # type: ignore[arg-type]


def test_turnover_fetch_rejects_bad_frequency() -> None:
    with pytest.raises(InvalidParameterError):
        riksbank_turnover_fetch(market="fx", frequency="yearly")  # type: ignore[arg-type]


@respx.mock
def test_turnover_fetch_empty_raises() -> None:
    respx.get(f"{_TURN}/markets/fi/frequencies/daily").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        riksbank_turnover_fetch(market="fi", frequency="daily")


# ---------------------------------------------------------------------------
# riksbank_holdings_fetch
# ---------------------------------------------------------------------------

_HOLD_AGG = [
    {
        "date": "2026-01-31",
        "security_group_name": "Government bonds",
        "security_group_name_se": "Statsobligationer",
        "balance_nominal_number": 76517000000.0,
    },
    {
        "date": "2026-01-31",
        "security_group_name": "Covered bonds",
        "security_group_name_se": "Säkerställda obligationer",
        "balance_nominal_number": 92800000000.0,
    },
]

_HOLD_DETAIL = [
    {
        "date": "2026-05-31",
        "security_group_name": "Covered bonds",
        "issuer_name": "SCBC",
        "security_id": "SCBC 151",
        "isin": "SE0013486156",
        "maturity_date": "2030-06-12",
        "balance_nominal_number": 3800000000.0,
    },
]


@respx.mock
def test_holdings_fetch_aggregated() -> None:
    respx.get(f"{_HOLD}/swedish_securities_aggregated").mock(return_value=httpx.Response(200, json=_HOLD_AGG))
    result = riksbank_holdings_fetch(dataset="swedish_securities_aggregated", start_date="2026-01-01")
    assert result.provenance.source == "riksbank_holdings_fetch"
    df = result.raw
    assert len(df) == 2
    assert set(df["dataset"]) == {"swedish_securities_aggregated"}
    assert df["date"].dtype.kind == "M"
    assert df["balance_nominal_number"].dtype.kind == "f"
    assert "Government bonds" in set(df["security_group_name"])


@respx.mock
def test_holdings_fetch_detail_passes_through_isin() -> None:
    respx.get(f"{_HOLD}/swedish_securities").mock(return_value=httpx.Response(200, json=_HOLD_DETAIL))
    result = riksbank_holdings_fetch(dataset="swedish_securities")
    df = result.raw
    assert df.iloc[0]["isin"] == "SE0013486156"
    assert df.iloc[0]["issuer_name"] == "SCBC"


def test_holdings_fetch_rejects_bad_dataset() -> None:
    with pytest.raises(InvalidParameterError):
        riksbank_holdings_fetch(dataset="foreign_securities")  # type: ignore[arg-type]


@respx.mock
def test_holdings_fetch_empty_raises() -> None:
    respx.get(f"{_HOLD}/swedish_securities_aggregated").mock(return_value=httpx.Response(200, json=[]))
    with pytest.raises(EmptyDataError):
        riksbank_holdings_fetch(dataset="swedish_securities_aggregated")


# ---------------------------------------------------------------------------
# enumerate_riksbank (multi-family)
# ---------------------------------------------------------------------------

_GROUPS_PAYLOAD = {
    "groupId": 1,
    "name": "Interest rates and exchange rates",
    "childGroups": [
        {"groupId": 130, "name": "Currencies against Swedish kronor", "childGroups": []},
        {"groupId": 2, "name": "Riksbank key interest rates", "childGroups": []},
    ],
}

_SERIES_PAYLOAD = [
    {
        "seriesId": "SECBREPOEFF",
        "source": "Sveriges Riksbank",
        "shortDescription": "Policy rate",
        "longDescription": "The policy rate is the Riksbank's most important interest rate.",
        "groupId": 2,
        "observationMinDate": "1994-06-01",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
    {
        "seriesId": "SEKEURPMI",
        "source": "Refinitiv",
        "shortDescription": "EUR",
        "midDescription": "Euro mid rate against the Swedish krona.",
        "groupId": 130,
        "observationMinDate": "1999-01-04",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
    {
        "seriesId": "SECBDISCEFF",
        "source": "Sveriges Riksbank",
        "shortDescription": "Discount rate",
        "midDescription": "Discount rate (historic).",
        "groupId": 999,
        "observationMinDate": "1907-11-11",
        "observationMaxDate": "2002-06-28",
        "seriesClosed": True,
    },
]

_MP_SERIES_PAYLOAD = {
    "data": [
        {
            "series_id": "SEQGDPNAYSA",
            "metadata": {
                "description": "GDP",
                "unit": "Annual percentage change",
                "source_agency": "Statistics Sweden and the Riksbank",
                "start_date": "1981-03-31",
                "note": "Seasonally adjusted.",
            },
        },
        {
            "series_id": "SEMCPIFNAYNA",
            "metadata": {
                "description": "CPIF",
                "unit": "Annual percentage change",
                "source_agency": "Statistics Sweden",
                "start_date": "1987-01-31",
            },
        },
    ]
}


def _mock_enumerate_endpoints() -> None:
    respx.get(f"{_SWEA}/Groups").mock(return_value=httpx.Response(200, json=_GROUPS_PAYLOAD))
    respx.get(f"{_SWEA}/Series").mock(return_value=httpx.Response(200, json=_SERIES_PAYLOAD))
    respx.get(f"{_MP}/series_ids").mock(return_value=httpx.Response(200, json=_MP_SERIES_PAYLOAD))


@respx.mock
def test_enumerate_exact_column_match() -> None:
    _mock_enumerate_endpoints()
    result = enumerate_riksbank()
    assert list(result.raw.columns) == [c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns]


@respx.mock
def test_enumerate_row_count_across_all_families() -> None:
    """3 SWEA + 7 SWESTR + 2 MP + 6 turnover + 2 holdings = 20."""
    _mock_enumerate_endpoints()
    result = enumerate_riksbank()
    assert len(result.raw) == len(_SERIES_PAYLOAD) + 7 + len(_MP_SERIES_PAYLOAD["data"]) + 6 + 2


@respx.mock
def test_enumerate_emits_all_five_sources() -> None:
    _mock_enumerate_endpoints()
    df = enumerate_riksbank().raw
    assert set(df["source"]) == {"swea", "swestr", "monetary_policy", "turnover", "holdings"}


@respx.mock
def test_enumerate_codes_route_each_family() -> None:
    _mock_enumerate_endpoints()
    df = enumerate_riksbank().raw

    # SWEA + SWESTR keep bare ids.
    assert (df["code"] == "SEKEURPMI").any()
    assert (df["code"] == "SWESTR").any()
    # The three new families carry routing prefixes.
    assert (df["code"] == "monetary_policy/SEQGDPNAYSA").any()
    assert set(df.loc[df["source"] == "turnover", "code"]) == {
        f"turnover/{m}/{f}" for m in ("fi", "fx", "ird") for f in ("daily", "monthly")
    }
    assert set(df.loc[df["source"] == "holdings", "code"]) == {
        "holdings/swedish_securities",
        "holdings/swedish_securities_aggregated",
    }


@respx.mock
def test_enumerate_monetary_policy_metadata_folds_into_description() -> None:
    _mock_enumerate_endpoints()
    df = enumerate_riksbank().raw
    gdp = df.loc[df["code"] == "monetary_policy/SEQGDPNAYSA"].iloc[0]
    assert gdp["title"] == "GDP (Annual percentage change)"
    assert gdp["frequency"] == "Quarterly"  # SE[Q]GDP...
    assert "Seasonally adjusted" in gdp["description"]
    assert gdp["provider"] == "Statistics Sweden and the Riksbank"
    cpif = df.loc[df["code"] == "monetary_policy/SEMCPIFNAYNA"].iloc[0]
    assert cpif["frequency"] == "Monthly"  # SE[M]CPIF...


@respx.mock
def test_enumerate_swea_group_and_frequency_resolution() -> None:
    _mock_enumerate_endpoints()
    df = enumerate_riksbank().raw
    repo = df.loc[df["code"] == "SECBREPOEFF"].iloc[0]
    assert "Riksbank key interest rates" in repo["group"]
    assert repo["frequency"] == "Daily"
    assert "policy rate" in repo["description"].lower()
    # Unknown group -> empty breadcrumb, not a crash.
    discount = df.loc[df["code"] == "SECBDISCEFF"].iloc[0]
    assert discount["group"] == ""
    assert bool(discount["series_closed"]) is True


@respx.mock
def test_enumerate_swestr_family_is_seven_bare_ids() -> None:
    _mock_enumerate_endpoints()
    df = enumerate_riksbank().raw
    swestr_rows = df[df["source"] == "swestr"]
    assert set(swestr_rows["code"]) == {
        "SWESTR",
        "SWESTRAVG1W",
        "SWESTRAVG1M",
        "SWESTRAVG2M",
        "SWESTRAVG3M",
        "SWESTRAVG6M",
        "SWESTRINDEX",
    }
    raw = swestr_rows[swestr_rows["code"] == "SWESTR"].iloc[0]
    assert "overnight" in raw["description"].lower()


@respx.mock
def test_enumerate_parse_error_on_bad_series_shape() -> None:
    respx.get(f"{_SWEA}/Groups").mock(return_value=httpx.Response(200, json=_GROUPS_PAYLOAD))
    respx.get(f"{_SWEA}/Series").mock(return_value=httpx.Response(200, json="not a list"))
    respx.get(f"{_MP}/series_ids").mock(return_value=httpx.Response(200, json=_MP_SERIES_PAYLOAD))
    with pytest.raises(ParseError):
        enumerate_riksbank()
