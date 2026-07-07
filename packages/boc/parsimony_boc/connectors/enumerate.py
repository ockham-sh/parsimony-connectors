"""BoC catalog enumerator (archetype A: the live ``/lists/series`` full index).

Emits one row per series **and** one row per *live* group. The series index
(``/lists/series/json``, ~15.6k) is the authoritative universe — verified
complete: a full fan-out over every group surfaces zero members absent from it.

The per-group fan-out (``/groups/{name}/json`` × ~2.4k) does double duty:

1. **Membership** — annotates each series with its group (97.7% of series carry
   a group; the first encountered wins for the rare multi-group series).
2. **Liveness** — a group whose detail endpoint 404s is *retired* (BoC leaves
   ~29 dated one-off panels in ``/lists/groups`` that 404 on both
   ``/groups/{name}`` and ``/observations/group/{name}``). We use the 404 signal
   to **prune** those group rows so the catalog never offers a panel that cannot
   be fetched. A *transient* failure (5xx / network) is kept best-effort — only
   a definitive 404 prunes.

A ``failed/total`` summary is logged at the end so a quietly-shrunk crawl is
visible (guidebook §7.2).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from parsimony.connector import enumerator
from parsimony.errors import ConnectorError, ParseError
from parsimony.transport import HttpClient, pooled_client
from parsimony.transport.helpers import fetch_json

from parsimony_boc._http import PROVIDER, make_valet_client
from parsimony_boc.outputs import BOC_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS

logger = logging.getLogger(__name__)


def _list_groups(client: HttpClient) -> dict[str, dict[str, Any]]:
    """Return BoC's group index (~2.4k entries) from ``/lists/groups/json``.

    This is the **bounding seam** for live tests: monkeypatch this module global
    to a 2–3 group slice so the per-group fan-out fires a handful of requests,
    never the full ~2,400-request crawl. ``enumerate_boc`` reads it as a module
    global at call time so the monkeypatch takes.
    """
    payload = fetch_json(client, path="lists/groups/json", op_name="groups/list")
    if not isinstance(payload, dict):
        raise ParseError(PROVIDER, "unexpected /lists/groups/json shape (expected object)")
    groups = payload.get("groups") or {}
    return groups if isinstance(groups, dict) else {}


def _fetch_group_membership(
    client: HttpClient,
    group_name: str,
) -> tuple[str, list[str], bool]:
    """Fetch one group's membership. Returns ``(name, members, alive)``.

    ``alive`` is ``False`` **only** on a definitive 404 (a retired group): the
    caller prunes those from the catalog. A transient transport error or a
    200-with-non-JSON body keeps ``alive=True`` (best-effort) — the group is
    still catalogued, it just loses its membership annotation this run, so a
    network blip never silently shrinks the catalog.
    """
    try:
        resp = client.request("GET", f"/groups/{group_name}/json", op_name="boc_enumerate")
        if resp.status_code == 404:
            # Retired-but-indexed group: prune it.
            return group_name, [], False
        elif resp.status_code >= 400:
            logger.warning("BoC group fetch failed for %r: HTTP %s", group_name, resp.status_code)
            return group_name, [], True
        body = resp.json()
    except (ConnectorError, ValueError) as exc:
        # Transport failure (mapped to a typed ConnectorError by request()) OR a
        # 200-with-non-JSON body (BoC sometimes serves an HTML stub) — keep the
        # group, drop only its membership this run.
        logger.warning("BoC group fetch failed for %r: %s", group_name, exc)
        return group_name, [], True

    if not isinstance(body, dict):
        return group_name, [], True
    details = body.get("groupDetails") or {}
    members = details.get("groupSeries") or {}
    if not isinstance(members, dict):
        return group_name, [], True
    return group_name, [s for s in members if s], True


def _build_series_to_group_map(
    client: HttpClient,
    groups_index: dict[str, dict[str, Any]],
) -> tuple[dict[str, tuple[str, str]], set[str], int]:
    """Fan out over groups → ``(series_to_group, dead_groups, transient_failures)``.

    ``series_to_group`` maps each series to its first ``(group_id, group_label)``.
    ``dead_groups`` are the names that 404'd (retired) — the caller skips their
    rows. ``transient_failures`` counts non-404 failures (kept, for the summary
    log). Connections are pooled across the walk via :func:`pooled_client`.
    """
    with pooled_client(client) as shared:
        results = [_fetch_group_membership(shared, name) for name in groups_index]

    series_to_group: dict[str, tuple[str, str]] = {}
    dead_groups: set[str] = set()
    transient_failures = 0
    for group_name, members, alive in results:
        if not alive:
            dead_groups.add(group_name)
            continue
        if not members:
            # alive but no members → either an empty/transient result. We can't
            # distinguish a live-but-empty group from a transient miss here, but
            # live groups always carry members in practice, so a non-404 empty is
            # a transient failure for summary purposes.
            transient_failures += 1
        info = groups_index.get(group_name) or {}
        label = (info.get("label") if isinstance(info, dict) else "") or ""
        for series_name in members:
            if series_name not in series_to_group:
                series_to_group[series_name] = (group_name, label)
    return series_to_group, dead_groups, transient_failures


@enumerator(output=BOC_ENUMERATE_OUTPUT, tags=["macro", "ca"])
def enumerate_boc() -> pd.DataFrame:
    """Enumerate every Bank of Canada series and live group via Valet.

    Granularity is one row per series — Valet addresses observations per series,
    so series-level keys are the right unit (~15.6k rows) — plus one row per live
    group (keyed ``group:NAME``) so whole panels are discoverable.

    Pipeline: ``/lists/series/json`` and ``/lists/groups/json``, then a
    serial ``/groups/{name}/json`` fan-out for series→group membership and
    group liveness (retired groups that 404 are pruned).
    """
    client = make_valet_client()

    series_payload = fetch_json(client, path="lists/series/json", op_name="series/list")
    if not isinstance(series_payload, dict):
        raise ParseError(PROVIDER, "unexpected /lists/series/json shape (expected object)")

    groups_index = _list_groups(client)
    series_to_group, dead_groups, transient_failures = _build_series_to_group_map(client, groups_index)

    series = series_payload.get("series") or {}
    if not isinstance(series, dict):
        series = {}

    rows: list[dict[str, str]] = []
    for series_name, info in series.items():
        if not series_name:
            continue
        if isinstance(info, dict):
            label = info.get("label") or series_name
            desc = info.get("description") or ""
        else:
            label = str(info)
            desc = ""

        group_id, group_label = series_to_group.get(series_name, ("", ""))
        rows.append(
            {
                "series_name": series_name,
                "title": label,
                "description": desc,
                "source": "valet",
                "entity_type": "series",
                "group": group_id,
                "group_label": group_label,
            }
        )

    n_series = len(rows)

    # One row per *live* group as a discoverable catalog entity. Groups are
    # addressable via ``boc_fetch(series_name="group:NAME")``; cataloguing them
    # lets agents search by the group-level description ("Month-end, Millions of
    # dollars") and fetch a whole panel in one shot. Retired groups (404 on
    # detail) are pruned so the catalog never offers an unfetchable panel.
    n_groups = 0
    for group_name, group_info in groups_index.items():
        if not group_name or group_name in dead_groups:
            continue
        if isinstance(group_info, dict):
            g_label = group_info.get("label") or group_name
            g_desc = group_info.get("description") or ""
        else:
            g_label = str(group_info)
            g_desc = ""
        rows.append(
            {
                "series_name": f"group:{group_name}",
                "title": g_label,
                "description": g_desc,
                "source": "valet",
                "entity_type": "group",
                "group": group_name,
                "group_label": g_label,
            }
        )
        n_groups += 1

    logger.info(
        "boc enumerate: %d series + %d live groups "
        "(%d retired groups pruned, %d transient membership failures of %d groups)",
        n_series,
        n_groups,
        len(dead_groups),
        transient_failures,
        len(groups_index),
    )

    columns = list(ENUMERATE_COLUMNS)
    return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)


__all__ = ["enumerate_boc"]
