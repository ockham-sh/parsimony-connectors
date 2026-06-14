"""SWESTR family — the Swedish Krona Short-Term Rate and its derivatives.

SWESTR is not in SWEA's ``/Series``: it lives on a separate product with its own URL
scheme (``/latest/<id>``, ``/avg/latest/<id>``, ``/index/latest/<id>``). The set of
series is small (seven) and stable — the overnight fixing, five compounded averages,
and one index — so a static registry is the right surface; it spares the enumerator a
second live API dependency for data that does not change.

(A historical ``PRESWESTR`` series of preliminary 2021 test-period values also exists
on the product but is deliberately excluded — it is superseded test data.)
"""

from __future__ import annotations

from typing import Any, Literal

from parsimony_riksbank.swea import to_value

# Closed enum on the connector signature. Typing the parameter as a Literal lets the
# framework reject bad values before any request rather than letting them 404.
SwestrSeries = Literal[
    "SWESTR",
    "SWESTRAVG1W",
    "SWESTRAVG1M",
    "SWESTRAVG2M",
    "SWESTRAVG3M",
    "SWESTRAVG6M",
    "SWESTRINDEX",
]

SWESTR_IDS: frozenset[str] = frozenset(
    {
        "SWESTR",
        "SWESTRAVG1W",
        "SWESTRAVG1M",
        "SWESTRAVG2M",
        "SWESTRAVG3M",
        "SWESTRAVG6M",
        "SWESTRINDEX",
    }
)

_PROVIDER = "Sveriges Riksbank"
_GROUP = "SWESTR — Swedish Krona Short-Term Rate"
_INCEPTION_DATE = "2021-09-01"  # Official launch of published values.

#: SWESTR series definitions. ``kind`` mirrors :func:`swestr_kind`.
_SWESTR_SERIES: tuple[dict[str, str], ...] = (
    {
        "series_id": "SWESTR",
        "kind": "rate",
        "unit": "Per cent",
        "title": "SWESTR — Swedish Krona Short-Term Rate",
        "description": (
            "SWESTR is a transaction-based reference rate calculated by the Riksbank from "
            "overnight unsecured money-market transactions in Swedish kronor. Published each "
            "banking day at 09:00 CET for the previous day. The official short-term "
            "interest-rate benchmark for Swedish kronor, designed to replace STIBOR T/N."
        ),
    },
    {
        "series_id": "SWESTRAVG1W",
        "kind": "avg",
        "unit": "Per cent",
        "title": "SWESTR — 1-week compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 1-week observation period. "
            "Published by the Riksbank at 09:05 CET each banking day, cumulative so the "
            "compounding effect is included."
        ),
    },
    {
        "series_id": "SWESTRAVG1M",
        "kind": "avg",
        "unit": "Per cent",
        "title": "SWESTR — 1-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 1-month observation period, "
            "published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRAVG2M",
        "kind": "avg",
        "unit": "Per cent",
        "title": "SWESTR — 2-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 2-month observation period, "
            "published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRAVG3M",
        "kind": "avg",
        "unit": "Per cent",
        "title": "SWESTR — 3-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 3-month observation period, "
            "published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRAVG6M",
        "kind": "avg",
        "unit": "Per cent",
        "title": "SWESTR — 6-month compounded average",
        "description": (
            "Compounded average of SWESTR over the most recent 6-month observation period, "
            "published daily by the Riksbank."
        ),
    },
    {
        "series_id": "SWESTRINDEX",
        "kind": "index",
        "unit": "Index",
        "title": "SWESTR Index",
        "description": (
            "SWESTR compounded index. The Riksbank publishes the index daily alongside the "
            "fixing; ratios between two index values yield the realised compounded rate "
            "between those dates, enabling clean settlement math for SWESTR-referencing "
            "instruments."
        ),
    },
)

#: ``series_id`` -> human title, for fetch-time title resolution.
TITLE_BY_ID: dict[str, str] = {spec["series_id"]: spec["title"] for spec in _SWESTR_SERIES}


def swestr_kind(series: str) -> str:
    """Return the URL family for a SWESTR series id.

    ``SWESTR`` -> raw fixing (``/latest`` or ``/all``); ``SWESTRAVG*`` -> compounded
    average (``/avg/...``); ``SWESTRINDEX`` -> index (``/index/...``).
    """
    if series == "SWESTR":
        return "rate"
    if series == "SWESTRINDEX":
        return "index"
    return "avg"


def parse_swestr_rows(series: str, payload: Any) -> list[dict[str, Any]]:
    """Flatten a SWESTR response into ``{series, date, value, ...}`` rows.

    The ``/latest`` endpoints return a single JSON object; the windowed endpoints
    return a JSON list. Raw SWESTR and the averages carry a ``rate`` field; the index
    uses ``value`` instead. Both normalise onto a single ``value`` column while the
    native confidence/volume metadata passes through on additional columns.
    """
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        date_val = item.get("date") or item.get("Date")
        if date_val is None:
            continue
        raw_value = item.get("rate")
        if raw_value is None:
            raw_value = item.get("value")
        row: dict[str, Any] = {"series": series, "date": date_val, "value": to_value(raw_value)}
        for extra in (
            "startDate",
            "publicationTime",
            "republication",
            "alternativeCalculation",
            "alternativeCalculationReason",
            "pctl12_5",
            "pctl87_5",
            "volume",
            "numberOfTransactions",
            "numberOfAgents",
        ):
            if extra in item:
                row[extra] = item[extra]
        rows.append(row)
    return rows


def build_swestr_rows() -> list[dict[str, Any]]:
    """One catalog row per :data:`_SWESTR_SERIES` entry (a static, I/O-free registry).

    Each row carries ``source="swestr"`` so a dispatching agent routes the hit to the
    SWESTR fetch verb, and a bare ``code`` equal to the series id.
    """
    rows: list[dict[str, Any]] = []
    for spec in _SWESTR_SERIES:
        rows.append(
            {
                "code": spec["series_id"],
                "title": spec["title"],
                "description": spec["description"],
                "source": "swestr",
                "frequency": "Daily",
                "unit": spec["unit"],
                "group": _GROUP,
                "provider": _PROVIDER,
                "observation_min": _INCEPTION_DATE,
                "observation_max": "",  # Live daily — no fixed end.
                "series_closed": False,
            }
        )
    return rows
