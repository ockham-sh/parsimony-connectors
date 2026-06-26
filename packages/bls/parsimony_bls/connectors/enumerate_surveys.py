"""``enumerate_bls_surveys`` — tier-1 feed: one row per BLS survey.

Lists every survey from the API ``/surveys`` endpoint (plain httpx — the API host
is not Akamai-walled, unlike the bulk download site). This drives the always-built
``bls_surveys`` discovery catalog; the dimension manifest is attached later, at
catalog-build time, for surveys that have a series catalog.
"""

from __future__ import annotations

import pandas as pd
from parsimony.connector import enumerator
from parsimony.errors import EmptyDataError
from parsimony.transport.helpers import fetch_json, make_http_client

from parsimony_bls._http import API_BASE, API_TIMEOUT
from parsimony_bls.outputs import BLS_SURVEYS_ENUM_OUTPUT
from parsimony_bls.surveys import HEADLINE_SURVEYS


@enumerator(output=BLS_SURVEYS_ENUM_OUTPUT, tags=["macro", "us"], secrets=("api_key",))
def enumerate_bls_surveys(api_key: str = "") -> pd.DataFrame:
    """List every BLS survey (program) for the tier-1 discovery catalog."""
    query = {"registrationkey": api_key} if api_key else None
    http = make_http_client(API_BASE, query_params=query, timeout=API_TIMEOUT)
    body = fetch_json(http, path="surveys", provider="bls", op_name="surveys")

    rows: list[dict[str, object]] = []
    for s in body.get("Results", {}).get("survey", []):
        code = (s.get("survey_abbreviation") or "").strip()
        if not code:
            continue
        rows.append(
            {
                "code": code,
                "title": (s.get("survey_name") or code).strip(),
                "survey": code,
                "has_series_catalog": code.upper() in HEADLINE_SURVEYS,
            }
        )

    if not rows:
        raise EmptyDataError("bls", query_params={"endpoint": "surveys"})

    return pd.DataFrame(rows)


__all__ = ["enumerate_bls_surveys"]
