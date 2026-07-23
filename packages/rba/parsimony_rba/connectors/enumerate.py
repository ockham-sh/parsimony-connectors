"""RBA catalog enumerator — the 3-pass HTML scrape (archetype E).

RBA publishes no machine-readable index, so the universe is the union of three
publication-format passes scraped from two index pages:

1. **CSV index** (``/statistics/tables/``): ~216 CSVs / ~3,958 active series — the
   bulk. Each CSV is one table sheet; the header block carries Title / Description /
   Frequency / Units / Series ID.
2. **Current XLSX-exclusive sheets** (``/statistics/tables/xls/``): the 5 current
   non-hist workbooks have, between them, exactly one sheet not re-exported as CSV
   (``a03`` → "Bond Purchase Program"). Detected by **dynamic exclusivity** — a
   workbook series is emitted only if its id is not already in the CSV-covered set —
   so the pass self-maintains instead of relying on a hardcoded sheet allow-list.
3. **Legacy xls-hist binaries** (``/statistics/historical-data.html``): ~26 ``.xls``
   workbooks of discontinued series (~200) that left the live CSVs.

The tables-page ``*hist.xlsx`` (~70) and the 11 period-range archives are deliberately
NOT crawled: a live audit proved they carry the same series ids as the current CSVs
(longer history only), so they add zero discoverable series.
"""

from __future__ import annotations

import logging

import pandas as pd
from parsimony.connector import enumerator

from parsimony_rba import _http, parsing
from parsimony_rba.outputs import _ENUMERATE_COLUMNS, RBA_ENUMERATE_OUTPUT

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Discovery seams (read at call time so live tests can bound the fan-out)
# ---------------------------------------------------------------------------


def _discover_csv_links(session: _http.Session) -> list[str]:
    """Scrape the tables index → the CSV link paths it advertises.

    The **primary bounding seam** for the live enumerate test: monkeypatch this to a
    1–2 link slice and the CSV fan-out fires a handful of requests instead of the
    full ~216-CSV crawl.
    """
    html = _http._curl_get(session, _http._TABLES_URL, op_name="tables_index")
    assert isinstance(html, str)
    return [m[0] for m in _http._CSV_LINK_PATTERN.findall(html)]


def _discover_xlsx_stems(session: _http.Session) -> set[str]:
    """Scrape the tables index → the XLSX workbook stems present (current + hist)."""
    html = _http._curl_get(session, _http._TABLES_URL, op_name="tables_index")
    assert isinstance(html, str)
    return set(_http._XLSX_LINK_PATTERN.findall(html))


def _discover_xls_hist_stems(session: _http.Session) -> list[str]:
    """Scrape the historical-data index → the xls-hist workbook stems."""
    try:
        hist_html = _http._curl_get(session, _http._HISTORICAL_URL, op_name="historical_index")
        assert isinstance(hist_html, str)
    except Exception:
        return []
    return sorted(set(_http._XLS_HIST_LINK_PATTERN.findall(hist_html)))


# ---------------------------------------------------------------------------
# Enumerator
# ---------------------------------------------------------------------------


@enumerator(output=RBA_ENUMERATE_OUTPUT, tags=["macro", "au"])
def enumerate_rba() -> pd.DataFrame:
    """Discover RBA series from the CSV index, current XLSX-exclusive sheets, and
    legacy xls-hist files.

    Compound catalog keys use ``table_id#series_id`` so duplicate series ids across
    tables remain addressable. One curl_cffi session is reused (Akamai-impersonated
    pooling) across all ~250 requests, run serially; per-fetch failures are skipped,
    not fatal.

    This is a ~250-request crawl of a bot-mitigated site and takes minutes. The RBA
    edge may start refusing connections during or shortly after it, which surfaces on
    the *next* call — including a cheap ``rba_fetch`` — as a sub-second connection
    failure. Nothing here caches that state; give the site a pause rather than
    retrying immediately. Prefer ``rba_search`` over re-enumerating.
    """
    all_rows: list[dict[str, str]] = []
    seen: set[str] = set()
    csv_series_ids: set[str] = set()

    def _merge(rows: list[dict[str, str]]) -> None:
        for row in rows:
            code = row["code"]
            if code not in seen:
                seen.add(code)
                all_rows.append(row)

    with _http._make_session() as session:
        # Step 1: discover the three link sets via module-global seams (each read at
        # call time so a live test can monkeypatch any of them to bound the fan-out).
        csv_links = _discover_csv_links(session)
        try:
            xlsx_stems = _discover_xlsx_stems(session)
        except Exception:
            xlsx_stems = set()

        if not csv_links and not xlsx_stems:
            return pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))

        # Step 2: CSV pass (serial). Also accumulate the covered series id set that
        # the XLSX-exclusivity pass diffs against.
        def _fetch_csv(link: str) -> list[dict[str, str]]:
            url = f"{_http._BASE_URL}{link}"
            try:
                text = _http._curl_get(session, url, op_name="csv_metadata")
                assert isinstance(text, str)
                return parsing._parse_csv_metadata(text, url)
            except Exception:
                return []

        for link in csv_links:
            rows = _fetch_csv(link)
            for row in rows:
                csv_series_ids.add(row["series_id"])
            _merge(rows)

        # Step 3: current XLSX-exclusive pass (dynamic). Only the current (non-hist)
        # workbooks; emit series not already covered by the CSV pass. Skipped if the
        # CSV pass produced nothing (a total CSV outage must not flood the catalog
        # with false-exclusives).
        current_xlsx = sorted(s for s in xlsx_stems if "hist" not in s)
        if current_xlsx and csv_series_ids:

            def _fetch_xlsx(stem: str) -> list[dict[str, str]]:
                url = f"{_http._BASE_URL}/statistics/tables/xls/{stem}.xlsx"
                try:
                    data = _http._curl_get(session, url, op_name="xlsx", binary=True)
                    assert isinstance(data, bytes)
                    return parsing._parse_xlsx_workbook_exclusive(data, stem, csv_series_ids)
                except Exception:
                    return []

            for stem in current_xlsx:
                _merge(_fetch_xlsx(stem))
        elif current_xlsx and not csv_series_ids:
            logger.warning("rba: skipping XLSX-exclusivity pass — CSV pass yielded no series")

        # Step 4: xls-hist legacy binary pass. The historical-data.html index lists
        # ~37 .xls workbooks; the period-range archives (digit-led stems) carry no new
        # series, so only the named (letter-led) workbooks are fetched.
        xls_hist_stems = _discover_xls_hist_stems(session)

        def _fetch_xls_hist(stem: str) -> list[dict[str, str]]:
            url = f"{_http._BASE_URL}/statistics/tables/xls-hist/{stem}.xls"
            try:
                data = _http._curl_get(session, url, op_name="xls_hist", binary=True)
                assert isinstance(data, bytes)
                return parsing._parse_xls_hist(data, stem)
            except Exception:
                return []

        hist_targets = [stem for stem in xls_hist_stems if stem and stem[0].isalpha()]
        for stem in hist_targets:
            _merge(_fetch_xls_hist(stem))

    return (
        pd.DataFrame(all_rows, columns=list(_ENUMERATE_COLUMNS))
        if all_rows
        else pd.DataFrame(columns=list(_ENUMERATE_COLUMNS))
    )
