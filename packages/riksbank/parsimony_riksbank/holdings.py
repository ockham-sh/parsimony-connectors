"""Holdings family — the Riksbank's holdings of Swedish securities.

The Riksbank publishes its securities holdings (built up through asset-purchase
programmes) as two datasets: ``swedish_securities`` (per-ISIN detail — issuer, ISIN,
maturity, nominal balance) and ``swedish_securities_aggregated`` (summed by security
group: government bonds/bills, covered/municipal/corporate bonds). Both update monthly
(government bonds run a two-month lag); a ``start_date`` bounds the window.

The provider's metadata endpoint advertises these as parquet files, but the data
endpoint itself serves **JSON** by default — so this family reads through the same
``fetch_json`` path as every other (no parquet/pyarrow dependency).

Two datasets, stable, so the catalog rows are a static registry.
"""

from __future__ import annotations

from typing import Any, Literal

HoldingsDataset = Literal["swedish_securities", "swedish_securities_aggregated"]

DATASETS: frozenset[str] = frozenset({"swedish_securities", "swedish_securities_aggregated"})

#: Earliest data — the Riksbank's modern holdings programme began with QE in 2015.
DEFAULT_START_DATE = "2015-01-01"

CODE_PREFIX = "holdings"
_GROUP = "Securities holdings"
_PROVIDER = "Sveriges Riksbank"

_DATASET_TITLE: dict[str, str] = {
    "swedish_securities": "Holdings in Swedish securities (per security)",
    "swedish_securities_aggregated": "Holdings in Swedish securities (aggregated by group)",
}

_DATASET_DESC: dict[str, str] = {
    "swedish_securities": (
        "The Riksbank's holdings of Swedish securities at the individual-security level: "
        "issuer, ISIN, security group (government bonds, government bills, covered bonds, "
        "municipal bonds, corporate bonds), maturity date and nominal balance. Updated "
        "monthly or quarterly by security group; government bonds run a two-month lag."
    ),
    "swedish_securities_aggregated": (
        "The Riksbank's holdings of Swedish securities aggregated by security group "
        "(government bonds, government bills, covered bonds, municipal bonds, corporate "
        "bonds): nominal balance per group per month. Updated the first business day of "
        "each month."
    ),
}


def build_holdings_rows() -> list[dict[str, Any]]:
    """One catalog row per holdings dataset (2 total)."""
    rows: list[dict[str, Any]] = []
    for dataset in ("swedish_securities", "swedish_securities_aggregated"):
        rows.append(
            {
                "code": f"{CODE_PREFIX}/{dataset}",
                "title": _DATASET_TITLE[dataset],
                "description": _DATASET_DESC[dataset],
                "source": "holdings",
                "frequency": "Monthly",
                "unit": "SEK nominal",
                "group": _GROUP,
                "provider": _PROVIDER,
                "observation_min": DEFAULT_START_DATE,
                "observation_max": "",
                "series_closed": False,
            }
        )
    return rows


def parse_holdings_rows(dataset: str, payload: Any) -> list[dict[str, Any]]:
    """Flatten a holdings response into ``{dataset, date, balance_nominal_number, ...}`` rows.

    The payload is a flat JSON list. ``date``/``balance_nominal_number`` become the
    named DATA columns; the descriptive columns (security group, issuer, ISIN, maturity)
    pass through so the per-security detail survives. The English ``security_group_name``
    is preferred for the title slot; the Swedish ``_se`` variant passes through.
    """
    items = payload if isinstance(payload, list) else []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        dt = item.get("date")
        if dt is None:
            continue
        balance = item.get("balance_nominal_number")
        try:
            balance_val: float | None = float(balance) if balance is not None else None
        except (TypeError, ValueError):
            balance_val = None
        row: dict[str, Any] = {
            "dataset": dataset,
            "date": dt,
            "balance_nominal_number": balance_val,
            "security_group_name": item.get("security_group_name"),
        }
        # Per-security detail columns (absent on the aggregated dataset) pass through.
        for extra in ("security_group_name_se", "issuer_name", "security_id", "isin", "maturity_date"):
            if extra in item:
                row[extra] = item[extra]
        rows.append(row)
    return rows
