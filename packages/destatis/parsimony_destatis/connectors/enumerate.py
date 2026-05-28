"""Destatis catalog enumeration connector."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pandas as pd
from parsimony.connector import enumerator
from parsimony_shared.cb_enumerate import DESCRIPTION_CHAR_CAP, ThrottledJsonFetcher

from parsimony_destatis._http import HEADERS, METADATA_CRAWL, get_path_json, looks_like_html
from parsimony_destatis.outputs import DESTATIS_ENUMERATE_OUTPUT, ENUMERATE_COLUMNS
from parsimony_destatis.params import DestatisEnumerateParams

logger = logging.getLogger(__name__)


def _pick_lang(node: Any, key: str = "name") -> tuple[str, str]:
    """Extract DE/EN strings from a Destatis ``{de, en}``-shaped node."""
    if not isinstance(node, dict):
        bare = (str(node).strip() if node is not None else "")
        return bare, bare

    nested = node.get(key)
    if isinstance(nested, dict):
        return (str(nested.get("de", "") or "").strip(), str(nested.get("en", "") or "").strip())

    de = str(node.get(f"{key}_de", "") or "").strip()
    en = str(node.get(f"{key}_en", "") or "").strip()
    if de or en:
        return de, en

    bare = str(node.get(key, "") or "").strip()
    return bare, bare


def _statistic_description(
    *,
    subject_area: str,
    name_de: str,
    name_en: str,
    description_de: str,
    n_tables: int,
) -> str:
    parts: list[str] = []
    if subject_area:
        parts.append(subject_area)
    if name_en and name_en != name_de:
        parts.append(f"{name_de} ({name_en})" if name_de else name_en)
    elif name_de:
        parts.append(name_de)
    if description_de:
        parts.append(description_de[:DESCRIPTION_CHAR_CAP])
    parts.append(f"German Federal Statistical Office (Destatis), {n_tables} tables.")
    return ". ".join(p for p in parts if p)


def _table_description(
    *,
    table_title: str,
    parent_code: str,
    parent_title_de: str,
    parent_title_en: str,
    parent_description_de: str,
    variable_names_en: list[str],
) -> str:
    parent_title = parent_title_en or parent_title_de
    parts: list[str] = []
    if table_title:
        parts.append(table_title)
    if parent_title:
        parts.append(f"Parent statistic: {parent_title} ({parent_code})")
    if parent_description_de:
        parts.append(parent_description_de[:DESCRIPTION_CHAR_CAP])
    if variable_names_en:
        parts.append(f"Variables: {', '.join(variable_names_en[:6])}")
    parts.append("Source: Destatis (Statistisches Bundesamt), GENESIS-Online.")
    return ". ".join(p for p in parts if p)


def _extract_variables(info_payload: dict[str, Any] | None) -> tuple[list[str], list[str]]:
    if not isinstance(info_payload, dict):
        return [], []
    variables = info_payload.get("variables") or info_payload.get("Variables") or []
    if not isinstance(variables, list):
        return [], []
    codes: list[str] = []
    names_en: list[str] = []
    for var in variables:
        if not isinstance(var, dict):
            continue
        code = str(var.get("code") or var.get("Code") or "").strip()
        if code:
            codes.append(code)
        _, en = _pick_lang(var, "name")
        if en:
            names_en.append(en)
    return codes, names_en


def _extract_statistics_list(index_payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(index_payload, list):
        return [s for s in index_payload if isinstance(s, dict)]
    if not isinstance(index_payload, dict):
        return []
    for key in ("statistics", "Statistics", "items", "Items"):
        candidate = index_payload.get(key)
        if isinstance(candidate, list):
            return [s for s in candidate if isinstance(s, dict)]
    return []


def _extract_tables_list(tables_payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(tables_payload, list):
        return [t for t in tables_payload if isinstance(t, dict)]
    if not isinstance(tables_payload, dict):
        return []
    for key in ("tables", "Tables", "items", "Items"):
        candidate = tables_payload.get(key)
        if isinstance(candidate, list):
            return [t for t in candidate if isinstance(t, dict)]
    return []


def _emit_rows_for_statistic(
    *,
    stat: dict[str, Any],
    info: dict[str, Any],
    tables_payload: dict[str, Any],
) -> list[dict[str, str]]:
    stat_code = str(stat.get("code") or stat.get("Code") or "").strip()
    name_de, name_en = _pick_lang(stat, "name")
    if not name_de and not name_en:
        name_de, name_en = _pick_lang(info, "name")

    subject_area = str(
        stat.get("subjectArea")
        or stat.get("subject_area")
        or stat.get("SubjectArea")
        or info.get("subjectArea")
        or ""
    ).strip()

    description_de = str(
        info.get("description", {}).get("de")
        if isinstance(info.get("description"), dict)
        else (info.get("description_de") or info.get("description") or "")
    ).strip()

    tables = _extract_tables_list(tables_payload)
    n_tables = len(tables)

    statistic_title = name_en or name_de or stat_code
    statistic_description = _statistic_description(
        subject_area=subject_area,
        name_de=name_de,
        name_en=name_en,
        description_de=description_de,
        n_tables=n_tables,
    )

    rows: list[dict[str, str]] = [
        {
            "code": stat_code,
            "title": statistic_title,
            "description": statistic_description,
            "entity_type": "statistic",
            "parent_statistic": "",
            "subject_area": subject_area,
            "title_de": name_de,
            "title_en": name_en,
            "variable_codes": "",
            "variable_names_en": "",
            "source": "genesis_online",
        }
    ]

    for table in tables:
        table_code = str(table.get("code") or table.get("Code") or "").strip()
        if not table_code:
            continue
        table_de, table_en = _pick_lang(table, "name")
        table_title = table_en or table_de or table_code
        var_codes, var_names_en = _extract_variables(table)

        rows.append(
            {
                "code": table_code,
                "title": table_title,
                "description": _table_description(
                    table_title=table_title,
                    parent_code=stat_code,
                    parent_title_de=name_de,
                    parent_title_en=name_en,
                    parent_description_de=description_de,
                    variable_names_en=var_names_en,
                ),
                "entity_type": "table",
                "parent_statistic": stat_code,
                "subject_area": subject_area,
                "title_de": table_de,
                "title_en": table_en,
                "variable_codes": ",".join(var_codes),
                "variable_names_en": ",".join(var_names_en),
                "source": "genesis_online",
            }
        )

    return rows


@enumerator(output=DESTATIS_ENUMERATE_OUTPUT, tags=["macro", "de"])
async def enumerate_destatis() -> pd.DataFrame:
    """Enumerate Destatis statistics and tables from GENESIS-Online."""
    DestatisEnumerateParams()

    rows: list[dict[str, str]] = []

    async with httpx.AsyncClient(timeout=60.0, headers=HEADERS) as client:
        fetcher = ThrottledJsonFetcher(
            client,
            provider="destatis",
            config=METADATA_CRAWL,
            logger=logger,
            accept_non_json=lambda r: not looks_like_html(r.text),
        )
        index = await get_path_json(fetcher, "/statistics")
        if index is None:
            logger.warning("Destatis enumerate: /statistics fetch failed; emitting empty catalog")
            return pd.DataFrame(columns=list(ENUMERATE_COLUMNS))

        statistics = _extract_statistics_list(index)
        if not statistics:
            logger.warning("Destatis enumerate: /statistics returned 0 entries")
            return pd.DataFrame(columns=list(ENUMERATE_COLUMNS))

        async def _gather_one(
            stat: dict[str, Any],
        ) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
            code = str(stat.get("code") or stat.get("Code") or "").strip()
            if not code:
                return stat, None, None
            info_task = get_path_json(fetcher, f"/statistics/{code}/information")
            tables_task = get_path_json(fetcher, f"/statistics/{code}/tables")
            info, tables = await asyncio.gather(info_task, tables_task)
            return stat, info, tables

        results = await asyncio.gather(*[_gather_one(s) for s in statistics])

    failed: list[str] = []
    for stat, info, tables_payload in results:
        stat_code = str(stat.get("code") or stat.get("Code") or "").strip()
        if not stat_code:
            continue
        if info is None and tables_payload is None:
            failed.append(stat_code)
            continue

        rows.extend(
            _emit_rows_for_statistic(
                stat=stat,
                info=info or {},
                tables_payload=tables_payload or {},
            )
        )

    if failed:
        logger.info(
            "Destatis enumerate: %d/%d statistics failed metadata fetch: %s",
            len(failed),
            len(statistics),
            ", ".join(failed[:20]),
        )
    else:
        logger.info("Destatis enumerate: %d statistics fetched successfully", len(statistics))

    df = pd.DataFrame(rows, columns=list(ENUMERATE_COLUMNS)) if rows else pd.DataFrame(columns=list(ENUMERATE_COLUMNS))
    return df
