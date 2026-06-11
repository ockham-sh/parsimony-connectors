"""Offline (respx-mocked) tests for the Riksbank connectors.

Riksbank's SWEA + SWESTR APIs are open / keyless for fetch and
enumeration; the ``Ocp-Apim-Subscription-Key`` header is optional and only
raises the quota. The connectors default ``api_key=""`` and skip the header
when empty, so there is no ``UnauthorizedError`` fast-fail on the fetch/
enumerate happy path — that fast-fail lives only on the *catalog-build*
path (see ``test_build_catalog.py``).
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError, InvalidParameterError
from parsimony.result import ColumnRole

from parsimony_riksbank import (
    CONNECTORS,
    RIKSBANK_ENUMERATE_OUTPUT,
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_swestr_fetch,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    assert names == {"riksbank_fetch", "riksbank_swestr_fetch", "enumerate_riksbank", "riksbank_search"}


def test_all_keyed_verbs_declare_api_key_secret() -> None:
    """The optional ``api_key`` is stripped from provenance on every keyed
    verb — even keyless, a passed key must not leak into recorded params."""
    for name in ("riksbank_fetch", "riksbank_swestr_fetch", "enumerate_riksbank"):
        conn = CONNECTORS[name]
        assert "api_key" in conn.secrets, f"{name} must declare api_key in secrets="


def test_enumerate_output_declares_description_and_source_columns() -> None:
    """Catalog-completeness contract: the enumerator emits a description
    metadata column for searchable prose and a ``source`` metadata column
    so dispatching agents route fetch calls without sniffing the series id.
    """
    by_role: dict[ColumnRole, list[str]] = {}
    for col in RIKSBANK_ENUMERATE_OUTPUT.columns:
        by_role.setdefault(col.role, []).append(col.name)
    assert "description" in by_role[ColumnRole.METADATA]
    assert "source" in by_role[ColumnRole.METADATA]


# ---------------------------------------------------------------------------
# riksbank_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_riksbank_fetch_latest_single_object() -> None:
    """SWEA ``/Observations/Latest/{id}`` returns a single JSON object (not
    a list). The connector wraps it into a one-row DataFrame."""
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI").mock(
        return_value=httpx.Response(200, json={"date": "2026-06-03", "value": 10.884})
    )
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(
        return_value=httpx.Response(
            200,
            json=[{"seriesId": "SEKEURPMI", "shortDescription": "EUR", "source": "Refinitiv"}],
        )
    )

    result = riksbank_fetch(series_id="SEKEURPMI")

    assert result.provenance.source == "riksbank_fetch"
    df = result.data
    assert len(df) == 1
    # Title resolved from /Series shortDescription (NOT the dead seriesName key).
    assert df.iloc[0]["title"] == "EUR"
    assert df.iloc[0]["value"] == pytest.approx(10.884)


@respx.mock
def test_riksbank_fetch_window_returns_list() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Observations/SEKEURPMI/2026-01-01/2026-01-10").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"date": "2026-01-02", "value": 10.8085},
                {"date": "2026-01-05", "value": 10.787},
            ],
        )
    )
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(
        return_value=httpx.Response(200, json=[{"seriesId": "SEKEURPMI", "shortDescription": "EUR"}])
    )

    result = riksbank_fetch(series_id="SEKEURPMI", from_date="2026-01-01", to_date="2026-01-10")
    df = result.data
    assert len(df) == 2
    assert list(df["date"].dt.strftime("%Y-%m-%d")) == ["2026-01-02", "2026-01-05"]


@respx.mock
def test_riksbank_fetch_title_lookup_failure_falls_back_to_id() -> None:
    """A transient operational failure of the secondary /Series title lookup
    must NOT fail the whole fetch — it falls back to the series id. (Replaces
    the old bare ``except Exception: pass`` swallow.)"""
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI").mock(
        return_value=httpx.Response(200, json={"date": "2026-06-03", "value": 10.884})
    )
    # /Series 429s — a typed ConnectorError the lookup tolerates.
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(return_value=httpx.Response(429, text="rate limited"))

    result = riksbank_fetch(series_id="SEKEURPMI")
    df = result.data
    assert len(df) == 1
    # Falls back to the id rather than raising or silently swallowing.
    assert df.iloc[0]["title"] == "SEKEURPMI"


@respx.mock
def test_riksbank_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/XX").mock(return_value=httpx.Response(200, json=[]))
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(return_value=httpx.Response(200, json=[]))

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
# enumerate_riksbank
# ---------------------------------------------------------------------------


_GROUPS_PAYLOAD = {
    # Mirrors the upstream shape: a single root node with ``childGroups``,
    # each node carrying ``groupId``/``name``/``description``.
    "groupId": 1,
    "name": "Interest rates and exchange rates",
    "description": "",
    "childGroups": [
        {
            "groupId": 11,
            "name": "Exchange rates",
            "description": "FX rates.",
            "childGroups": [
                {"groupId": 130, "name": "Currencies against Swedish kronor", "childGroups": []},
                {"groupId": 133, "name": "Monthly aggregate", "childGroups": []},
            ],
        },
        {
            "groupId": 137,
            "name": "Riksbank interest rates",
            "childGroups": [
                {"groupId": 2, "name": "Riksbank key interest rates", "childGroups": []},
            ],
        },
    ],
}

_SERIES_PAYLOAD = [
    {
        "seriesId": "SECBREPOEFF",
        "source": "Sveriges Riksbank",
        "shortDescription": "Policy rate",
        "midDescription": "Policy rate",
        "longDescription": "The policy rate is the interest rate at which the banks can borrow or deposit in the Riksbank.",  # noqa: E501
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
        "longDescription": "Mid rate fixed daily at 16:15 CET.",
        "groupId": 130,
        "observationMinDate": "1999-01-04",
        "observationMaxDate": "2026-04-24",
        "seriesClosed": False,
    },
    {
        "seriesId": "SEKUSDPMM",
        "source": "Refinitiv",
        "shortDescription": "USD monthly average",
        "midDescription": "USD/SEK monthly average.",
        "longDescription": "",  # missing → fall through to mid
        "groupId": 133,
        "observationMinDate": "1990-01-31",
        "observationMaxDate": "2026-03-31",
        "seriesClosed": False,
    },
    {
        "seriesId": "SECBDISCEFF",
        "source": "Sveriges Riksbank",
        "shortDescription": "Discount rate",
        "midDescription": "Discount rate (historic).",
        "longDescription": "Replaced by the reference rate in 2002.",
        "groupId": 999,  # unknown group — should resolve to "" gracefully
        "observationMinDate": "1907-11-11",
        "observationMaxDate": "2002-06-28",
        "seriesClosed": True,
    },
]


def _mock_swea_endpoints() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Groups").mock(return_value=httpx.Response(200, json=_GROUPS_PAYLOAD))
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(return_value=httpx.Response(200, json=_SERIES_PAYLOAD))


@respx.mock
def test_enumerate_exact_column_match() -> None:
    """@enumerator enforces an EXACT column match against the declared schema."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data
    assert list(df.columns) == [c.name for c in RIKSBANK_ENUMERATE_OUTPUT.columns]


@respx.mock
def test_enumerate_riksbank_emits_description_for_embedder() -> None:
    """Every row carries upstream long-form text on a DESCRIPTION column."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data
    assert "description" in df.columns

    repo = df.loc[df["series_id"] == "SECBREPOEFF"].iloc[0]
    assert "policy rate" in repo["description"].lower()
    # Title resolves from shortDescription.
    assert repo["title"] == "Policy rate"

    # Series with empty longDescription falls back to midDescription.
    usd_monthly = df.loc[df["series_id"] == "SEKUSDPMM"].iloc[0]
    assert usd_monthly["description"] == "USD/SEK monthly average."


@respx.mock
def test_enumerate_riksbank_emits_source_metadata_for_dispatch() -> None:
    """Catalog rows carry ``source="swea"`` or ``source="swestr"`` so a
    dispatching agent knows which fetch connector to call."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data
    assert set(df["source"].unique()) == {"swea", "swestr"}


@respx.mock
def test_enumerate_riksbank_resolves_group_hierarchy() -> None:
    """Group resolution walks ``childGroups`` into a full breadcrumb path."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data

    repo = df.loc[df["series_id"] == "SECBREPOEFF"].iloc[0]
    assert "Riksbank key interest rates" in repo["group"]
    assert repo["group"].startswith("Interest rates and exchange rates")

    eur = df.loc[df["series_id"] == "SEKEURPMI"].iloc[0]
    assert "Currencies against Swedish kronor" in eur["group"]

    # Unknown groupId resolves to empty string rather than raising.
    discount = df.loc[df["series_id"] == "SECBDISCEFF"].iloc[0]
    assert discount["group"] == ""


@respx.mock
def test_enumerate_riksbank_infers_frequency_with_provenance_tag() -> None:
    """``frequency_source`` reports how the value was derived: group beats
    suffix beats the unknown fallback."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data

    repo = df.loc[df["series_id"] == "SECBREPOEFF"].iloc[0]
    assert repo["frequency"] == "Daily"
    assert repo["frequency_source"] == "group"

    eur = df.loc[df["series_id"] == "SEKEURPMI"].iloc[0]
    assert eur["frequency"] == "Daily"
    assert eur["frequency_source"] == "group"

    usd_monthly = df.loc[df["series_id"] == "SEKUSDPMM"].iloc[0]
    assert usd_monthly["frequency"] == "Monthly"
    assert usd_monthly["frequency_source"] == "group"

    # group=999 unknown → suffix EFF unmatched → "Unknown".
    discount = df.loc[df["series_id"] == "SECBDISCEFF"].iloc[0]
    assert discount["frequency"] == "Unknown"
    assert discount["frequency_source"] == "unknown"


@respx.mock
def test_enumerate_riksbank_passes_through_provider_and_date_range() -> None:
    """Upstream ``source`` (Refinitiv, etc.) and observation bounds become
    METADATA columns; ``series_closed`` carries the lifecycle bit."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data

    eur = df.loc[df["series_id"] == "SEKEURPMI"].iloc[0]
    assert eur["provider"] == "Refinitiv"
    assert eur["observation_min"] == "1999-01-04"
    assert eur["observation_max"] == "2026-04-24"
    assert bool(eur["series_closed"]) is False

    discount = df.loc[df["series_id"] == "SECBDISCEFF"].iloc[0]
    assert bool(discount["series_closed"]) is True


@respx.mock
def test_enumerate_riksbank_row_count_matches_series_payload() -> None:
    """One row per upstream SWEA series plus the static SWESTR registry
    (seven entries). Fixture trims SWEA to four, so 4 + 7 = 11."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    assert len(result.data) == len(_SERIES_PAYLOAD) + 7


@respx.mock
def test_enumerate_riksbank_emits_swestr_family() -> None:
    """The SWESTR family (fixing + five compounded averages + index) appears
    as seven rows with ``source="swestr"``. The ids exactly match what
    :func:`riksbank_swestr_fetch` accepts."""
    _mock_swea_endpoints()
    result = enumerate_riksbank()
    df = result.data

    swestr_rows = df[df["source"] == "swestr"]
    assert len(swestr_rows) == 7
    expected_ids = {
        "SWESTR",
        "SWESTRAVG1W",
        "SWESTRAVG1M",
        "SWESTRAVG2M",
        "SWESTRAVG3M",
        "SWESTRAVG6M",
        "SWESTRINDEX",
    }
    assert set(swestr_rows["series_id"]) == expected_ids
    raw = swestr_rows[swestr_rows["series_id"] == "SWESTR"].iloc[0]
    assert "overnight" in raw["description"].lower()
    assert raw["frequency"] == "Daily"
    assert raw["frequency_source"] == "registry"


@respx.mock
def test_enumerate_riksbank_parse_error_on_bad_series_shape() -> None:
    """A 200 with a non-list /Series body → ParseError (not a crash)."""
    from parsimony.errors import ParseError

    respx.get("https://api.riksbank.se/swea/v1/Groups").mock(return_value=httpx.Response(200, json=_GROUPS_PAYLOAD))
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(return_value=httpx.Response(200, json="not a list"))
    with pytest.raises(ParseError):
        enumerate_riksbank()


# ---------------------------------------------------------------------------
# riksbank_swestr_fetch
# ---------------------------------------------------------------------------


@respx.mock
def test_riksbank_swestr_fetch_latest_rate_hits_latest_endpoint() -> None:
    """With no date window, the raw SWESTR fixing goes to ``/latest/SWESTR``
    and returns a single-row DataFrame with the published rate."""
    respx.get("https://api.riksbank.se/swestr/v1/latest/SWESTR").mock(
        return_value=httpx.Response(
            200,
            json={
                "rate": 1.639,
                "date": "2026-04-23",
                "pctl12_5": 1.55,
                "pctl87_5": 1.65,
                "volume": 74459,
                "numberOfTransactions": 255,
                "numberOfAgents": 6,
                "publicationTime": "2026-04-24T07:00:00Z",
                "republication": False,
                "alternativeCalculation": False,
                "alternativeCalculationReason": None,
            },
        )
    )
    result = riksbank_swestr_fetch(series="SWESTR")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["value"] == pytest.approx(1.639)
    assert df.iloc[0]["series"] == "SWESTR"
    assert df.iloc[0]["title"] == "SWESTR — Swedish Krona Short-Term Rate"
    # Native metadata folds in as additional columns.
    assert df.iloc[0]["numberOfTransactions"] == 255


@respx.mock
def test_riksbank_swestr_fetch_windowed_average_hits_avg_endpoint() -> None:
    """A compounded average with a date window routes to ``/avg/<id>``."""
    respx.get("https://api.riksbank.se/swestr/v1/avg/SWESTRAVG1W").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"rate": 1.633, "date": "2026-04-15", "startDate": "2026-04-08", "republication": False},
                {"rate": 1.636, "date": "2026-04-16", "startDate": "2026-04-09", "republication": False},
            ],
        )
    )
    result = riksbank_swestr_fetch(series="SWESTRAVG1W", from_date="2026-04-15", to_date="2026-04-16")
    df = result.data
    assert len(df) == 2
    assert list(df["series"].unique()) == ["SWESTRAVG1W"]
    assert df.iloc[0]["startDate"] == "2026-04-08"


@respx.mock
def test_riksbank_swestr_fetch_index_normalises_value_field() -> None:
    """The SWESTR index publishes ``value`` (an index level) rather than
    ``rate``; the connector normalises both onto a single ``value`` column."""
    respx.get("https://api.riksbank.se/swestr/v1/index/latest/SWESTRINDEX").mock(
        return_value=httpx.Response(
            200,
            json={"value": 110.25032277, "date": "2026-04-24", "republication": False},
        )
    )
    result = riksbank_swestr_fetch(series="SWESTRINDEX")
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["value"] == pytest.approx(110.25032277)


@respx.mock
def test_riksbank_swestr_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://api.riksbank.se/swestr/v1/latest/SWESTR").mock(return_value=httpx.Response(200, json={}))
    with pytest.raises(EmptyDataError):
        riksbank_swestr_fetch(series="SWESTR")


def test_swestr_fetch_rejects_lonely_from_date() -> None:
    """``from_date`` without ``to_date`` → InvalidParameterError (inline)."""
    with pytest.raises(InvalidParameterError):
        riksbank_swestr_fetch(series="SWESTR", from_date="2026-01-01")
