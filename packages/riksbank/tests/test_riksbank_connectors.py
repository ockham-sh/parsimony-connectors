"""Happy-path tests for the Riksbank connectors.

Riksbank exposes an optional Ocp-Apim-Subscription-Key header; the connector
defaults ``api_key=""`` (quota lower without a key but the endpoint works).
Template 401/429 contract targets keyword-only deps that are required —
Riksbank's api_key is optional, so we don't exercise the 401/429 mapping
here.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import EmptyDataError
from parsimony.result import ColumnRole

from parsimony_riksbank import (
    CONNECTORS,
    RIKSBANK_ENUMERATE_OUTPUT,
    RiksbankEnumerateParams,
    RiksbankFetchParams,
    RiksbankSwestrFetchParams,
    enumerate_riksbank,
    riksbank_fetch,
    riksbank_swestr_fetch,
)

# ---------------------------------------------------------------------------
# Plugin contract shape
# ---------------------------------------------------------------------------


def test_connectors_collection_exposes_expected_names() -> None:
    names = {c.name for c in CONNECTORS}
    # Forecasts endpoints 404 as of catalog freeze; CBA is
    # un-discoverable on api.riksbank.se. The dispatch ``source``
    # METADATA column is in place so ``riksbank_forecasts_fetch`` and
    # ``riksbank_cba_fetch`` slot in without a schema migration when
    # those endpoints surface.
    assert names == {"riksbank_fetch", "riksbank_swestr_fetch", "enumerate_riksbank", "riksbank_search"}


def test_enumerate_output_declares_description_and_source_columns() -> None:
    """Catalog-completeness contract: the enumerator must emit a
    DESCRIPTION column (so its text feeds the embedder via
    ``semantic_text()``, not just BM25) and a ``source`` METADATA
    column (so dispatching agents can route fetch calls without
    sniffing the series id).
    """
    by_role: dict[ColumnRole, list[str]] = {}
    for col in RIKSBANK_ENUMERATE_OUTPUT.columns:
        by_role.setdefault(col.role, []).append(col.name)
    assert by_role.get(ColumnRole.DESCRIPTION) == ["description"]
    assert "source" in by_role[ColumnRole.METADATA]


# ---------------------------------------------------------------------------
# riksbank_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_fetch_returns_observations() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/SEKEURPMI").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"date": "2026-04-17", "value": 11.35},
                {"date": "2026-04-18", "value": 11.40},
            ],
        )
    )
    # /Series title lookup — returned but optional
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(
        return_value=httpx.Response(
            200,
            json=[{"seriesId": "SEKEURPMI", "seriesName": "SEK/EUR exchange rate"}],
        )
    )

    bound = riksbank_fetch.bind(api_key="")
    result = await bound(RiksbankFetchParams(series_id="SEKEURPMI"))

    assert result.provenance.source == "riksbank"
    df = result.data
    assert len(df) == 2
    assert df.iloc[0]["title"] == "SEK/EUR exchange rate"


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://api.riksbank.se/swea/v1/Observations/Latest/XX").mock(
        return_value=httpx.Response(200, json=[])
    )

    bound = riksbank_fetch.bind(api_key="")
    with pytest.raises(EmptyDataError):
        await bound(RiksbankFetchParams(series_id="XX"))


def test_fetch_rejects_empty_series_id() -> None:
    with pytest.raises(ValueError):
        RiksbankFetchParams(series_id="   ")


# NOTE: the existing `_both_dates_or_neither` validator is decorated with
# @field_validator but does not pass validate_default=True, so it does not
# fire when `to_date` takes its None default with `from_date` set. That is a
# pre-existing bug — documented here rather than fixed mid-sweep to keep
# per-package commits focused on migration-only changes.


# ---------------------------------------------------------------------------
# enumerate_riksbank
# ---------------------------------------------------------------------------


_GROUPS_PAYLOAD = {
    # Mirrors the upstream shape: a single root node with ``childGroups``,
    # not the ``groupInfos``/``children`` keys earlier code looked for.
    "groupId": 1,
    "name": "Interest rates and exchange rates",
    "childGroups": [
        {
            "groupId": 11,
            "name": "Exchange rates",
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
        "longDescription": "",  # description is missing → fall through to mid
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
    respx.get("https://api.riksbank.se/swea/v1/Groups").mock(
        return_value=httpx.Response(200, json=_GROUPS_PAYLOAD)
    )
    respx.get("https://api.riksbank.se/swea/v1/Series").mock(
        return_value=httpx.Response(200, json=_SERIES_PAYLOAD)
    )


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_riksbank_emits_description_for_embedder() -> None:
    """Every row carries the upstream long-form text on a DESCRIPTION
    column, so the catalog embedder sees the phrase at index time."""
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
    df = result.data
    assert "description" in df.columns

    repo = df.loc[df["series_id"] == "SECBREPOEFF"].iloc[0]
    assert "policy rate" in repo["description"].lower()

    # Series with empty longDescription falls back to midDescription.
    usd_monthly = df.loc[df["series_id"] == "SEKUSDPMM"].iloc[0]
    assert usd_monthly["description"] == "USD/SEK monthly average."


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_riksbank_emits_source_metadata_for_dispatch() -> None:
    """Catalog rows carry ``source="swea"`` (SWEA fetches) or
    ``source="swestr"`` (SWESTR fetches) so an agent dispatching off a
    hit knows which fetch connector to call. Forecasts/CBA are not
    implemented; when they land, additional rows will carry
    ``"forecasts"``/``"cba"`` without a schema migration.
    """
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
    df = result.data
    assert set(df["source"].unique()) == {"swea", "swestr"}


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_riksbank_resolves_group_hierarchy() -> None:
    """Group resolution must walk ``childGroups`` (the actual upstream
    key). Earlier code looked for ``groupInfos`` and lost every group
    label silently."""
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
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
@pytest.mark.asyncio
async def test_enumerate_riksbank_infers_frequency_with_provenance_tag() -> None:
    """``frequency_source`` reports how the value was derived. Group-id
    matches beat suffix matches; suffix matches beat the unknown
    fallback. Downstream consumers can choose how much to trust each.
    """
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
    df = result.data

    # group=2 → Daily via group lookup, regardless of suffix shape.
    repo = df.loc[df["series_id"] == "SECBREPOEFF"].iloc[0]
    assert repo["frequency"] == "Daily"
    assert repo["frequency_source"] == "group"

    # group=130 → Daily via group lookup (matches the SEKEURPMI suffix
    # heuristic too, but group wins because it's the more confident path).
    eur = df.loc[df["series_id"] == "SEKEURPMI"].iloc[0]
    assert eur["frequency"] == "Daily"
    assert eur["frequency_source"] == "group"

    # group=133 → Monthly via group lookup.
    usd_monthly = df.loc[df["series_id"] == "SEKUSDPMM"].iloc[0]
    assert usd_monthly["frequency"] == "Monthly"
    assert usd_monthly["frequency_source"] == "group"

    # group=999 → unknown → suffix EFF → "Unknown" frequency.
    discount = df.loc[df["series_id"] == "SECBDISCEFF"].iloc[0]
    assert discount["frequency"] == "Unknown"
    assert discount["frequency_source"] == "unknown"


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_riksbank_passes_through_provider_and_date_range() -> None:
    """Upstream ``source`` (Refinitiv, Nasdaq, etc.) and observation
    bounds become METADATA columns that BM25 can match on, and
    ``series_closed`` carries the lifecycle bit."""
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
    df = result.data

    eur = df.loc[df["series_id"] == "SEKEURPMI"].iloc[0]
    assert eur["provider"] == "Refinitiv"
    assert eur["observation_min"] == "1999-01-04"
    assert eur["observation_max"] == "2026-04-24"
    # pandas widens Python ``bool`` to ``numpy.bool_`` inside a
    # DataFrame column; comparing with ``==`` keeps the assertion
    # robust to that promotion (``is False`` would mis-fire).
    assert bool(eur["series_closed"]) is False

    discount = df.loc[df["series_id"] == "SECBDISCEFF"].iloc[0]
    assert bool(discount["series_closed"]) is True


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_riksbank_row_count_matches_series_payload() -> None:
    """Sanity check: one row per upstream SWEA series plus the static
    SWESTR registry (seven entries — fixing, five compounded averages,
    one index). SWEA returns ~117 series live; the test fixture trims
    to four for clarity, so the expected total here is 4 + 7 = 11.
    """
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
    assert len(result.data) == len(_SERIES_PAYLOAD) + 7


@respx.mock
@pytest.mark.asyncio
async def test_enumerate_riksbank_emits_swestr_family() -> None:
    """The SWESTR family (fixing + five compounded averages + index)
    appears as seven rows with ``source="swestr"``. The series ids
    exactly match what :func:`riksbank_swestr_fetch` accepts — if they
    diverge, an agent finding a catalog hit could not actually fetch it.
    """
    _mock_swea_endpoints()
    bound = enumerate_riksbank.bind(api_key="")
    result = await bound(RiksbankEnumerateParams())
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
    # Description text is rich enough to index on — spot-check a phrase
    # the embedder should pick up for the raw fixing.
    raw = swestr_rows[swestr_rows["series_id"] == "SWESTR"].iloc[0]
    assert "overnight" in raw["description"].lower()
    assert raw["frequency"] == "Daily"
    assert raw["frequency_source"] == "registry"


# ---------------------------------------------------------------------------
# riksbank_swestr_fetch
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_swestr_fetch_latest_rate_hits_latest_endpoint() -> None:
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
    bound = riksbank_swestr_fetch.bind(api_key="")
    result = await bound(RiksbankSwestrFetchParams(series="SWESTR"))
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["value"] == pytest.approx(1.639)
    assert df.iloc[0]["series"] == "SWESTR"
    # Native metadata columns ride along so analysts can spot
    # alternative-calculation days without a second request.
    assert df.iloc[0]["numberOfTransactions"] == 255


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_swestr_fetch_windowed_average_hits_avg_endpoint() -> None:
    """A compounded average with a date window routes to ``/avg/<id>``
    (not ``/all`` or ``/latest``) and flattens the list response."""
    respx.get("https://api.riksbank.se/swestr/v1/avg/SWESTRAVG1W").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "rate": 1.633,
                    "date": "2026-04-15",
                    "startDate": "2026-04-08",
                    "publicationTime": "2026-04-16T07:05:00Z",
                    "republication": False,
                },
                {
                    "rate": 1.636,
                    "date": "2026-04-16",
                    "startDate": "2026-04-09",
                    "publicationTime": "2026-04-17T07:05:00Z",
                    "republication": False,
                },
            ],
        )
    )
    bound = riksbank_swestr_fetch.bind(api_key="")
    result = await bound(
        RiksbankSwestrFetchParams(
            series="SWESTRAVG1W",
            from_date="2026-04-15",
            to_date="2026-04-16",
        )
    )
    df = result.data
    assert len(df) == 2
    assert list(df["series"].unique()) == ["SWESTRAVG1W"]
    # ``startDate`` is the window-start for a compounded average; the
    # raw fixing doesn't publish it. Keeping it on the DataFrame lets
    # agents reason about the underlying accrual interval.
    assert df.iloc[0]["startDate"] == "2026-04-08"


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_swestr_fetch_index_normalises_value_field() -> None:
    """The SWESTR index publishes ``value`` (an index level) rather than
    ``rate``. The connector normalises both field names onto a single
    ``value`` column so downstream code doesn't branch on series kind.
    """
    respx.get("https://api.riksbank.se/swestr/v1/index/latest/SWESTRINDEX").mock(
        return_value=httpx.Response(
            200,
            json={
                "value": 110.25032277,
                "date": "2026-04-24",
                "publicationTime": "2026-04-24T07:05:00Z",
                "republication": False,
            },
        )
    )
    bound = riksbank_swestr_fetch.bind(api_key="")
    result = await bound(RiksbankSwestrFetchParams(series="SWESTRINDEX"))
    df = result.data
    assert len(df) == 1
    assert df.iloc[0]["value"] == pytest.approx(110.25032277)


@respx.mock
@pytest.mark.asyncio
async def test_riksbank_swestr_fetch_raises_empty_data_when_no_observations() -> None:
    respx.get("https://api.riksbank.se/swestr/v1/latest/SWESTR").mock(
        return_value=httpx.Response(200, json={})
    )
    bound = riksbank_swestr_fetch.bind(api_key="")
    from parsimony.errors import EmptyDataError as _EmptyDataError

    with pytest.raises(_EmptyDataError):
        await bound(RiksbankSwestrFetchParams(series="SWESTR"))


def test_swestr_fetch_rejects_unknown_series() -> None:
    """Closed enum: pydantic rejects unknown ids at param-validation time."""
    with pytest.raises(ValueError):
        RiksbankSwestrFetchParams(series="SWESTRAVGBOGUS")  # type: ignore[arg-type]


def test_swestr_fetch_rejects_lonely_from_date() -> None:
    """``from_date`` without ``to_date`` is ambiguous against the
    window vs. latest dispatch. The validator rejects it."""
    with pytest.raises(ValueError):
        RiksbankSwestrFetchParams(series="SWESTR", from_date="2026-01-01")
