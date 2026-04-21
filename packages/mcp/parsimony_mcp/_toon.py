"""TOON (Token-Oriented Object Notation) encoder for parsimony Results.

TOON is a YAML-like, CSV-row-bodied format optimized for LLM token
efficiency. For tabular data it spends column names ONCE in a header
rather than repeating them per row (as JSON does), which is a
30-50% token saving for typical preview tables.

The format we emit for a DataFrame:

    preview[50]{id,title,date}:
      GDPC1,Real GDP,1947-01-01
      UNRATE,Unemployment Rate,1948-01-01

    total_rows: 1234
    truncation: "Discovery preview only — for the full 1234 rows, …"

The format we emit for a Series:

    result[2]{key,value}:
      gdp,1234.5
      unemployment,3.5

The format we emit for a scalar:

    value: "Just a string"

This module replaces the previous Markdown-table-with-per-cell-
sanitization rendering in bridge.py. Markdown's structural
fragility (a cell containing ``|`` breaks the row; a cell containing
``\\n\\n**SYSTEM**:`` breaks the host/data boundary) required four
escape passes per cell (`_sanitize_cell`); TOON's CSV-style quoting
subsumes all of them with a single quote-when-needed rule, so
``_sanitize_cell`` is gone entirely.

Per-cell length is still capped at ``_MAX_CELL_CHARS`` because a
malicious upstream returning a 100KB cell would blow the agent's
context budget regardless of format.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

_MAX_CELL_CHARS = 500
_SPECIAL_CHARS = (",", '"', "\n", "\r", ":")


def encode_dataframe(df: pd.DataFrame, *, name: str = "rows") -> str:
    """Encode a DataFrame as a TOON tabular block.

    The header line is ``{name}[{n_rows}]{{col1,col2,...}}:`` with
    column names quoted only when they contain special characters.
    Each row is two-space-indented and comma-joined with the same
    quoting rule.
    """
    n = len(df)
    cols = [_quote(c) for c in df.columns]
    header = f"{name}[{n}]{{{','.join(cols)}}}:"
    if n == 0:
        return header
    column_list = list(df.columns)
    rows = [
        "  " + ",".join(_quote(row[col]) for col in column_list)
        for _, row in df.iterrows()
    ]
    return header + "\n" + "\n".join(rows)


def encode_series(series: pd.Series, *, name: str = "result") -> str:
    """Encode a Series as a 2-column TOON tabular block (key, value)."""
    df = series.to_frame(name="value").reset_index()
    df.columns = ["key", "value"]
    return encode_dataframe(df, name=name)


def encode_kv(key: str, value: Any) -> str:
    """Encode a single top-level ``key: value`` line."""
    return f"{key}: {_quote(value)}"


def _quote(value: Any) -> str:
    """Quote a TOON field value if needed.

    Per the TOON spec (CSV-style):

    * ``None`` → empty string (the TOON convention for missing).
    * ``bool`` → ``true`` / ``false`` unquoted.
    * Numeric → ``str(value)`` unquoted.
    * String → quoted with ``"…"`` and internal ``"`` doubled if it
      contains a comma, double quote, newline, carriage return, or
      colon (the structural delimiters), OR has leading/trailing
      whitespace, OR is empty.

    Per-cell content is truncated at ``_MAX_CELL_CHARS`` regardless
    of type to bound the agent's context budget against compromised
    upstreams.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Pass numeric values through; pandas/numpy types stringify
        # cleanly via str().
        return str(value)
    s = str(value)
    if len(s) > _MAX_CELL_CHARS:
        s = s[: _MAX_CELL_CHARS - 1] + "…"
    needs_quoting = (
        s == ""
        or s != s.strip()
        or any(c in s for c in _SPECIAL_CHARS)
    )
    if needs_quoting:
        return '"' + s.replace('"', '""') + '"'
    return s
