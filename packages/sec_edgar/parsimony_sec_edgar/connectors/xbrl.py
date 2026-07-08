"""SEC EDGAR XBRL verbs: per-company concept history, raw facts, cross-company frames.

The three XBRL APIs aggregate facts that use a non-custom taxonomy (``us-gaap``,
``ifrs-full``, ``dei``, ``srt``) and apply to the whole filing entity, so they
are comparable across companies and over time:

* ``company_concept`` — one concept's full disclosure history for one company
  (the closest thing EDGAR has to a timeseries).
* ``company_facts``  — every concept a company has reported (a deep raw blob).
* ``frames``         — one concept, one calendrical period, across every
  reporting company (a cross-sectional snapshot).
"""

from __future__ import annotations

import re
from typing import Any, cast

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError, InvalidParameterError, ParseError
from parsimony.transport.helpers import fetch_json

from parsimony_sec_edgar._http import PROVIDER, data_client, normalize_cik
from parsimony_sec_edgar.outputs import (
    COMPANY_CONCEPT_COLUMNS,
    COMPANY_CONCEPT_OUTPUT,
    FRAMES_COLUMNS,
    FRAMES_OUTPUT,
)

# Frame period: CY#### (annual), CY####Q# (quarterly), CY####Q#I (instantaneous).
_PERIOD_RE = re.compile(r"^CY\d{4}(?:Q[1-4]I?)?$")


@connector(output=COMPANY_CONCEPT_OUTPUT, tags=["sec_edgar", "tool"])
def sec_edgar_company_concept(
    cik: str, tag: str, taxonomy: str = "us-gaap", unit: str | None = None
) -> pd.DataFrame:
    """Fetch the full reported history of one XBRL financial concept for a company.

    Returns a tidy long timeseries — one row per disclosed fact: period end +
    value + unit + fiscal year/period + form + filed date + accession (+ period
    start for duration concepts). `tag` is an XBRL concept (e.g. "Revenues",
    "Assets", "AccountsPayableCurrent"); `taxonomy` defaults to "us-gaap" (also
    "dei", "ifrs-full", "srt"); `unit` filters to one unit of measure (e.g.
    "USD") when a company reports the concept in several.
    """
    cik_norm = normalize_cik(cik)
    tag_s = (tag or "").strip()
    if not tag_s:
        raise InvalidParameterError(PROVIDER, "tag is required (an XBRL concept, e.g. 'Assets')")
    tax = (taxonomy or "us-gaap").strip()

    payload = fetch_json(
        data_client(),
        path=f"/api/xbrl/companyconcept/CIK{cik_norm}/{tax}/{tag_s}.json",
        op_name="company_concept",
    )
    units = payload.get("units", {}) if isinstance(payload, dict) else {}
    if not isinstance(units, dict) or not units:
        raise EmptyDataError(
            PROVIDER,
            message=f"No XBRL facts for {tax}:{tag_s} (CIK {cik_norm})",
            query_params={"cik": cik_norm, "taxonomy": tax, "tag": tag_s},
        )

    rows: list[dict[str, Any]] = []
    for unit_name, facts in units.items():
        if unit and unit_name != unit:
            continue
        for fact in facts if isinstance(facts, list) else []:
            rows.append(
                {
                    "end": fact.get("end"),
                    "val": fact.get("val"),
                    "unit": unit_name,
                    "fy": fact.get("fy"),
                    "fp": fact.get("fp"),
                    "form": fact.get("form"),
                    "filed": fact.get("filed"),
                    "accn": fact.get("accn"),
                    "start": fact.get("start"),
                }
            )
    if not rows:
        raise EmptyDataError(
            PROVIDER,
            message=f"No facts for unit {unit!r} of {tax}:{tag_s} (CIK {cik_norm})",
            query_params={"cik": cik_norm, "taxonomy": tax, "tag": tag_s, "unit": unit},
        )
    return pd.DataFrame(rows)[list(COMPANY_CONCEPT_COLUMNS)]


@connector(tags=["sec_edgar", "tool"])
def sec_edgar_company_facts(cik: str) -> dict[str, Any]:
    """Return the raw XBRL company-facts blob for a CIK.

    Fetches /api/xbrl/companyfacts/CIK{cik}.json — the full set of reported
    financial concepts keyed by taxonomy (us-gaap, dei, …). Returned verbatim
    as a dict for downstream extraction. (For one concept's history as a tidy
    table, use `sec_edgar_company_concept`.)

    Nested shape: facts[taxonomy][concept] = {label, description, units:
    {unit_code: [{end, val, fy, fp, form, …}]}} — hundreds of concepts per CIK.
    """
    cik_norm = normalize_cik(cik)
    payload = fetch_json(
        data_client(),
        path=f"/api/xbrl/companyfacts/CIK{cik_norm}.json",
        op_name="company_facts",
    )
    if not isinstance(payload, dict) or not payload.get("facts"):
        raise EmptyDataError(
            PROVIDER,
            message=f"No XBRL company facts returned for CIK {cik_norm}",
            query_params={"cik": cik_norm},
        )
    return cast(dict[str, Any], payload)


@connector(output=FRAMES_OUTPUT, tags=["sec_edgar", "tool"])
def sec_edgar_frames(
    tag: str, period: str, unit: str = "USD", taxonomy: str = "us-gaap"
) -> pd.DataFrame:
    """Fetch one XBRL concept for one period across every reporting company.

    Returns a cross-sectional snapshot — one row per company that reported `tag`
    in `period`: cik + entity name + value + period end + location (+ start).
    `period` is `CY####` (annual), `CY####Q#` (quarterly), or `CY####Q#I`
    (instantaneous, e.g. "CY2023Q1I"); `unit` defaults to "USD" (ratios use
    "USD-per-shares", dimensionless uses "pure"); `taxonomy` defaults to "us-gaap".
    """
    tag_s = (tag or "").strip()
    if not tag_s:
        raise InvalidParameterError(PROVIDER, "tag is required (an XBRL concept, e.g. 'Assets')")
    period_s = (period or "").strip()
    if not _PERIOD_RE.match(period_s):
        raise InvalidParameterError(
            PROVIDER, "period must be like 'CY2023' (annual), 'CY2023Q1' (quarterly), or 'CY2023Q1I' (instantaneous)"
        )
    tax = (taxonomy or "us-gaap").strip()
    uom = (unit or "USD").strip()

    payload = fetch_json(
        data_client(),
        path=f"/api/xbrl/frames/{tax}/{tag_s}/{uom}/{period_s}.json",
        op_name="frames",
    )
    data = payload.get("data", []) if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise ParseError(PROVIDER, "frames response did not contain a data array")
    if not data:
        raise EmptyDataError(
            PROVIDER,
            message=f"No companies reported {tax}:{tag_s} ({uom}) for {period_s}",
            query_params={"tag": tag_s, "period": period_s, "unit": uom, "taxonomy": tax},
        )

    rows = [
        {
            "cik": normalize_cik(str(pt.get("cik", ""))),
            "entityName": pt.get("entityName"),
            "val": pt.get("val"),
            "end": pt.get("end"),
            "loc": pt.get("loc"),
            "accn": pt.get("accn"),
            "start": pt.get("start"),
        }
        for pt in data
        if isinstance(pt, dict)
    ]
    return pd.DataFrame(rows)[list(FRAMES_COLUMNS)]
