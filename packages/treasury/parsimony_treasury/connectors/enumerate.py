"""Treasury catalog enumerator — live Fiscal Data metadata (A) + ODM registry (D).

The authoritative Fiscal Data universe is the live ``/services/dtg/metadata/`` JSON (the
same source the fiscaldata SPA consumes): one GET returns every dataset → endpoint →
field, so the catalog self-tracks new datasets (archetype A). One row per measure field
(``{endpoint}#{column}``). The famous daily interest-rate series are not in that metadata —
they are appended from the curated ODM registry (archetype D, ``home/{feed}#{column}``).
"""

from __future__ import annotations

import pandas as pd
from parsimony.connector import enumerator
from parsimony.transport.helpers import fetch_json

from parsimony_treasury import _http, parsing
from parsimony_treasury.outputs import _ENUMERATE_COLUMNS, TREASURY_ENUMERATE_OUTPUT
from parsimony_treasury.rate_feeds import build_treasury_rate_rows


def _list_datasets() -> list[dict]:
    """The discovery seam: GET ``/services/dtg/metadata/`` → the datasets list.

    Read at call time so a live/offline test can monkeypatch it to a 1–3 dataset slice and
    bound the work instead of pulling the full 1.2 MB metadata payload.
    """
    raw = fetch_json(
        _http.metadata_client(),
        path=_http.METADATA_PATH,
        op_name="datasets/metadata",
    )
    return parsing.unwrap_metadata(raw)


@enumerator(output=TREASURY_ENUMERATE_OUTPUT, tags=["macro", "us"])
def enumerate_treasury() -> pd.DataFrame:
    """Enumerate Treasury Fiscal Data measures and ODM rate-feed benchmarks.

    Combines one row per addressable Fiscal Data measure (across every dataset's
    endpoints) with the static Office of Debt Management yield- and bill-rate series, for
    catalog indexing. Each row is an ``{endpoint}#{field}`` code with a title, definition,
    and routing ``source``.
    """
    datasets = _list_datasets()
    rows = parsing.fiscal_measure_rows(datasets)
    rows.extend(build_treasury_rate_rows())

    # @enumerator drops unmapped columns then requires an EXACT match — build the frame
    # with exactly the declared columns, in order.
    columns = list(_ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
