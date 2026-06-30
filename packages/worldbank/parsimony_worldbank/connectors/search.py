"""World Bank indicator search connector.

Search the World Bank indicator catalogue by keyword. Returns matching
indicator series codes and names — perfect for discovering the ``indicator_id``
to pass to ``worldbank_fetch``.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pandas as pd
from parsimony.connector import connector
from parsimony.errors import EmptyDataError
from parsimony.transport.helpers import fetch_json, make_http_client

from parsimony_worldbank._http import BASE_URL, HEADERS
from parsimony_worldbank.outputs import SEARCH_COLUMNS, WORLDBANK_SEARCH_OUTPUT

logger = logging.getLogger(__name__)

MAX_PER_PAGE = 100


@connector(output=WORLDBANK_SEARCH_OUTPUT, tags=["macro", "international", "development"])
def worldbank_search(
    query: Annotated[str, "Search keyword(s)"],
    per_page: int = 50,
) -> pd.DataFrame:
    """Search World Bank indicator catalogue.

    Parameters
    ----------
    query:
        Free-text search string (e.g. ``"GDP"``, ``"population"``).
        Passed as the ``search`` parameter to the World Bank API.
    per_page:
        Number of results per page (max 100). Defaults to 50.

    Returns
    -------
    pd.DataFrame
        One row per matching indicator with columns ``indicator_id``,
        ``indicator_name``, ``source_note``, ``source_org``, ``page``.
    """
    query = query.strip()
    if not query:
        raise EmptyDataError(
            "worldbank",
            message="Search query must be non-empty",
        )

    if per_page < 1:
        per_page = 1
    elif per_page > MAX_PER_PAGE:
        per_page = MAX_PER_PAGE

    client = make_http_client(BASE_URL, headers=HEADERS, timeout=30.0)

    all_rows: list[dict[str, Any]] = []
    page = 1

    while page <= 100:  # safety valve
        params: dict[str, Any] = {
            "format": "json",
            "search": query,
            "per_page": per_page,
            "page": page,
        }

        data = fetch_json(
            client,
            path="/indicator",
            params=params,
            provider="worldbank",
            op_name=f"search_page_{page}",
        )

        if not isinstance(data, list) or len(data) < 2:
            break

        records = data[1]
        if not isinstance(records, list) or not records:
            break

        for record in records:
            if not isinstance(record, dict):
                continue
            all_rows.append(
                {
                    "indicator_id": str(record.get("id", "")),
                    "indicator_name": str(record.get("name", "")),
                    "source_note": str(record.get("sourceNote", "")),
                    "source_org": str(record.get("sourceOrganization", "")),
                    "page": page,
                }
            )

        # Check if there are more pages
        meta = data[0]
        if isinstance(meta, dict):
            total = meta.get("total", 0)
            per = meta.get("per_page", per_page)
            if isinstance(total, (int, float)) and isinstance(per, (int, float)):
                total_pages = -(-int(total) // int(per))  # ceil division
                if page >= total_pages:
                    break

        page += 1

    if not all_rows:
        raise EmptyDataError(
            "worldbank",
            message=f"No indicators match query={query!r}",
            query_params={"query": query},
        )

    return pd.DataFrame(all_rows, columns=list(SEARCH_COLUMNS))
