"""Global equity screener — orchestration, enrichment fan-out, post-filter.

The screener is the one FMP connector with non-trivial internal shape. It
fans out to three FMP endpoints (``company-screener``, ``key-metrics-ttm``,
``ratios-ttm``), joins the results, applies a residual pandas ``.query()``
filter, sorts, limits, and projects. The ``@connector`` stub lives in
``__init__.py`` so the registry generator picks it up via AST; the real
work lives here.
"""

from __future__ import annotations

import asyncio
import difflib
import logging
import re
from typing import Any

import pandas as pd
from parsimony.errors import EmptyDataError, ParseError, PaymentRequiredError, UnauthorizedError
from parsimony.http import HttpClient
from parsimony.result import Provenance, Result

from parsimony_fmp._http import fetch_json, pooled_client
from parsimony_fmp.outputs import SCREENER_OUTPUT
from parsimony_fmp.params import FmpScreenerParams

logger = logging.getLogger(__name__)


# Single cap for concurrent FMP enrichment requests per screener invocation.
# FMP rate-limits on a per-account basis; 10 is a conservative steady-state
# ceiling that exercises connection pooling without triggering 429. A single
# shared semaphore (rather than one-per-endpoint) correctly represents the
# shared upstream rate budget.
DEFAULT_ENRICHMENT_CONCURRENCY: int = 10


# ---------------------------------------------------------------------------
# Column-source classification (determines which enrichment endpoints to hit)
# ---------------------------------------------------------------------------


_SCREENER_NATIVE_COLS: frozenset[str] = frozenset(
    {
        "symbol",
        "companyName",
        "sector",
        "industry",
        "country",
        "exchange",
        "exchangeShortName",
        "marketCap",
        "price",
        "beta",
        "volume",
        "lastAnnualDividend",
        "isEtf",
        "isFund",
        "isActivelyTrading",
    }
)

_KEY_METRICS_TTM_COLS: frozenset[str] = frozenset(
    {
        # Enterprise value
        "enterpriseValueTTM",
        "evToSalesTTM",
        "evToEBITDATTM",
        "evToOperatingCashFlowTTM",
        "evToFreeCashFlowTTM",
        # Profitability
        "returnOnEquityTTM",
        "returnOnAssetsTTM",
        "returnOnInvestedCapitalTTM",
        "returnOnCapitalEmployedTTM",
        "operatingReturnOnAssetsTTM",
        "returnOnTangibleAssetsTTM",
        "earningsYieldTTM",
        "freeCashFlowYieldTTM",
        "incomeQualityTTM",
        # Leverage / liquidity
        "netDebtToEBITDATTM",
        "currentRatioTTM",
        "taxBurdenTTM",
        "interestBurdenTTM",
        "workingCapitalTTM",
        "investedCapitalTTM",
        # Capital efficiency
        "capexToOperatingCashFlowTTM",
        "capexToDepreciationTTM",
        "capexToRevenueTTM",
        "stockBasedCompensationToRevenueTTM",
        "salesGeneralAndAdministrativeToRevenueTTM",
        "researchAndDevelopementToRevenueTTM",
        "intangiblesToTotalAssetsTTM",
        # Working capital days
        "daysOfSalesOutstandingTTM",
        "daysOfPayablesOutstandingTTM",
        "daysOfInventoryOutstandingTTM",
        "operatingCycleTTM",
        "cashConversionCycleTTM",
        # Balance sheet ($)
        "averageReceivablesTTM",
        "averagePayablesTTM",
        "averageInventoryTTM",
        "freeCashFlowToEquityTTM",
        "freeCashFlowToFirmTTM",
        "tangibleAssetValueTTM",
        "netCurrentAssetValueTTM",
        # Graham
        "grahamNumberTTM",
        "grahamNetNetTTM",
    }
)

_FINANCIAL_RATIOS_TTM_COLS: frozenset[str] = frozenset(
    {
        # Margins
        "grossProfitMarginTTM",
        "ebitdaMarginTTM",
        "ebitMarginTTM",
        "operatingProfitMarginTTM",
        "netProfitMarginTTM",
        "pretaxProfitMarginTTM",
        "bottomLineProfitMarginTTM",
        "continuousOperationsProfitMarginTTM",
        # Valuation multiples
        "priceToEarningsRatioTTM",
        "priceToBookRatioTTM",
        "priceToSalesRatioTTM",
        "priceToFreeCashFlowRatioTTM",
        "priceToOperatingCashFlowRatioTTM",
        "priceToEarningsGrowthRatioTTM",
        "forwardPriceToEarningsGrowthRatioTTM",
        "priceToFairValueTTM",
        "enterpriseValueMultipleTTM",
        # Efficiency
        "receivablesTurnoverTTM",
        "payablesTurnoverTTM",
        "inventoryTurnoverTTM",
        "assetTurnoverTTM",
        "fixedAssetTurnoverTTM",
        "workingCapitalTurnoverRatioTTM",
        # Liquidity
        "quickRatioTTM",
        "cashRatioTTM",
        "solvencyRatioTTM",
        # Debt
        "debtToEquityRatioTTM",
        "debtToAssetsRatioTTM",
        "debtToCapitalRatioTTM",
        "longTermDebtToCapitalRatioTTM",
        "financialLeverageRatioTTM",
        "debtToMarketCapTTM",
        # Coverage
        "interestCoverageRatioTTM",
        "debtServiceCoverageRatioTTM",
        "operatingCashFlowRatioTTM",
        "operatingCashFlowSalesRatioTTM",
        "operatingCashFlowCoverageRatioTTM",
        "freeCashFlowOperatingCashFlowRatioTTM",
        "capitalExpenditureCoverageRatioTTM",
        "shortTermOperatingCashFlowCoverageRatioTTM",
        "dividendPaidAndCapexCoverageRatioTTM",
        # Dividends
        "dividendPayoutRatioTTM",
        "dividendYieldTTM",
        # Per-share ($)
        "revenuePerShareTTM",
        "netIncomePerShareTTM",
        "bookValuePerShareTTM",
        "tangibleBookValuePerShareTTM",
        "freeCashFlowPerShareTTM",
        "operatingCashFlowPerShareTTM",
        "cashPerShareTTM",
        "shareholdersEquityPerShareTTM",
        "capexPerShareTTM",
        "interestDebtPerShareTTM",
        # Other
        "effectiveTaxRateTTM",
        "netIncomePerEBTTTM",
        "ebtPerEbitTTM",
    }
)


# Pushdown map: screener-param name → FMP screener endpoint param name.
_PUSHDOWN_MAP: dict[str, str] = {
    "market_cap_min": "marketCapMoreThan",
    "market_cap_max": "marketCapLowerThan",
    "sector": "sector",
    "industry": "industry",
    "beta_min": "betaMoreThan",
    "beta_max": "betaLowerThan",
    "price_min": "priceMoreThan",
    "price_max": "priceLowerThan",
    "dividend_min": "dividendMoreThan",
    "dividend_max": "dividendLowerThan",
    "volume_min": "volumeMoreThan",
    "volume_max": "volumeLowerThan",
    "exchange": "exchange",
    "country": "country",
    "is_etf": "isEtf",
    "is_fund": "isFund",
    "is_actively_trading": "isActivelyTrading",
}


def _col_refs_source(
    sort_by: str | None,
    where_clause: str | None,
    fields: list[str] | None,
    col_set: frozenset[str],
) -> bool:
    """Return True if any column reference overlaps ``col_set``.

    Used to skip enrichment calls when the caller's ``fields``, ``sort_by``
    and ``where_clause`` only reference screener-native columns — a load-
    bearing optimisation that turns 4000+ HTTP calls into zero.
    """
    if sort_by and sort_by in col_set:
        return True
    if where_clause and any(col in where_clause for col in col_set):
        return True
    return bool(fields and any(f in col_set for f in fields))


def _extract_unknown_cols(error_msg: str, allowed: list[str]) -> list[str]:
    """Best-effort pull of column names mentioned in a pandas query error."""
    names = re.findall(r"'([A-Za-z_]\w*)'", error_msg)
    return [n for n in names if n not in allowed]


def _collect_enrichment(results: list[Any]) -> pd.DataFrame:
    """Merge per-symbol enrichment results into one DataFrame.

    Raises immediately on auth / payment errors (non-transient, do not
    continue). Logs and skips other per-symbol failures so a few bad
    symbols don't doom an otherwise-healthy batch.
    """
    dfs: list[pd.DataFrame] = []
    errors: list[BaseException] = []
    for r in results:
        if isinstance(r, pd.DataFrame) and not r.empty:
            dfs.append(r)
        elif isinstance(r, BaseException):
            errors.append(r)

    for err in errors:
        if isinstance(err, (UnauthorizedError, PaymentRequiredError)):
            raise err

    if errors:
        logger.warning(
            "fmp_screener: %d/%d enrichment requests failed (first: %s)",
            len(errors),
            len(results),
            errors[0],
        )

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=["symbol"])


async def _fetch_enrichment_df(
    semaphore: asyncio.Semaphore,
    http: HttpClient,
    path: str,
    symbol: str,
) -> pd.DataFrame:
    """Fetch one enrichment endpoint for one symbol, return a DataFrame.

    Runs inside the shared semaphore to respect the per-invocation
    concurrency cap. Returns an empty DataFrame for genuinely empty
    responses so ``_collect_enrichment`` can distinguish those from errors.
    """
    async with semaphore:
        data = await fetch_json(http, path=path, params={"symbol": symbol}, op_name=f"fmp_screener:{path}")
    if not data:
        return pd.DataFrame()
    df = pd.json_normalize(data if isinstance(data, list) else [data])
    if df.empty:
        return pd.DataFrame()
    if "symbol" not in df.columns:
        df.insert(0, "symbol", symbol)
    return df


async def execute(params: FmpScreenerParams, http: HttpClient) -> Result:
    """Run the full screener pipeline and return a :class:`Result`.

    Called from the ``@connector fmp_screener`` stub in ``__init__.py``.
    """
    where_clause = params.where_clause
    limit = params.limit
    sort_by = params.sort_by
    sort_order = (params.sort_order or "desc").lower()
    fields = params.fields
    screener_limit = int(params.prefilter_limit or max(limit or 500, 500))

    # Step 1: Screener pushdown
    screener_params: dict[str, Any] = {"limit": screener_limit}
    params_dict = params.model_dump(
        exclude={"where_clause", "sort_by", "sort_order", "limit", "prefilter_limit", "fields"}
    )
    for internal_key, upstream_key in _PUSHDOWN_MAP.items():
        value = params_dict.get(internal_key)
        if value is not None:
            screener_params[upstream_key] = value

    screener_raw = await fetch_json(
        http,
        path="company-screener",
        params=screener_params,
        op_name="fmp_screener",
    )
    screener_df = pd.json_normalize(screener_raw) if screener_raw else pd.DataFrame()
    if screener_df.empty:
        raise EmptyDataError(
            provider="fmp",
            message="FMP company-screener returned no rows for the selected filter set.",
        )
    if "symbol" not in screener_df.columns:
        raise ParseError(
            provider="fmp",
            message="Unexpected company-screener payload: missing 'symbol' column.",
        )

    symbols = [s for s in screener_df["symbol"].dropna().astype(str).str.strip().tolist() if s]
    if not symbols:
        raise EmptyDataError(
            provider="fmp",
            message="FMP company-screener did not return any valid symbols.",
        )

    # Step 2: Decide which enrichment endpoints are required
    need_metrics = fields is None or _col_refs_source(sort_by, where_clause, fields, _KEY_METRICS_TTM_COLS)
    need_ratios = fields is None or _col_refs_source(sort_by, where_clause, fields, _FINANCIAL_RATIOS_TTM_COLS)

    logger.info(
        "fmp_screener: enrichment plan — %d symbols, metrics=%s, ratios=%s",
        len(symbols),
        need_metrics,
        need_ratios,
    )

    # Step 3: Enrichment fan-out with a single shared semaphore and a
    # single pooled httpx.AsyncClient. One semaphore correctly models the
    # shared upstream rate budget across metrics+ratios endpoints.
    metrics_df = pd.DataFrame(columns=["symbol"])
    ratios_df = pd.DataFrame(columns=["symbol"])

    if need_metrics or need_ratios:
        semaphore = asyncio.Semaphore(DEFAULT_ENRICHMENT_CONCURRENCY)
        async with pooled_client(http) as enrich_http:
            gathered_metrics: list[Any] = []
            gathered_ratios: list[Any] = []
            tasks: list[asyncio.Task[pd.DataFrame]] = []
            if need_metrics:
                metrics_tasks = [
                    asyncio.create_task(_fetch_enrichment_df(semaphore, enrich_http, "key-metrics-ttm", s))
                    for s in symbols
                ]
                tasks.extend(metrics_tasks)
            if need_ratios:
                ratios_tasks = [
                    asyncio.create_task(_fetch_enrichment_df(semaphore, enrich_http, "ratios-ttm", s))
                    for s in symbols
                ]
                tasks.extend(ratios_tasks)

            all_results = await asyncio.gather(*tasks, return_exceptions=True)

            if need_metrics:
                gathered_metrics = list(all_results[: len(symbols)])
                all_results = all_results[len(symbols) :]
            if need_ratios:
                gathered_ratios = list(all_results[: len(symbols)])

        if need_metrics:
            metrics_df = _collect_enrichment(gathered_metrics)
        if need_ratios:
            ratios_df = _collect_enrichment(gathered_ratios)

    # Step 4: Join enrichment into screener base
    df = screener_df.drop_duplicates("symbol")
    if need_metrics and not metrics_df.empty:
        if "marketCap" in metrics_df.columns:
            metrics_df = metrics_df.drop(columns=["marketCap"])
        df = pd.merge(df, metrics_df, on="symbol", how="left")
    if need_ratios and not ratios_df.empty:
        dup_cols = [c for c in ratios_df.columns if c != "symbol" and c in df.columns]
        if dup_cols:
            ratios_df = ratios_df.drop(columns=dup_cols)
        df = pd.merge(df, ratios_df, on="symbol", how="left")

    # Step 5: Residual filtering. Alphabetical sort of allowed_cols keeps
    # the "Available columns (N): [...]" error text deterministic across
    # runs — an agent-feedback loop the MCP host depends on.
    allowed_cols = sorted(df.columns.tolist())
    if where_clause:
        try:
            df = df.query(where_clause)
        except Exception as exc:
            unknown = _extract_unknown_cols(str(exc), allowed_cols)
            suggestions = {c: difflib.get_close_matches(c, allowed_cols, n=3, cutoff=0.6) for c in unknown}
            sug_str = "; ".join(
                f"'{c}' → {v}" if v else f"'{c}' → no close match" for c, v in suggestions.items()
            )
            raise ValueError(
                f"Invalid where_clause: {exc}\n"
                + (f"Unknown column(s): {unknown}. Suggestions: {sug_str}\n" if unknown else "")
                + f"Available columns ({len(allowed_cols)}): {allowed_cols}"
            ) from exc

    # Step 6: Sort and limit
    if sort_by is not None:
        if sort_by not in df.columns:
            sort_suggestions = difflib.get_close_matches(sort_by, allowed_cols, n=5, cutoff=0.6)
            raise ValueError(
                f"Invalid sort_by column: '{sort_by}'. "
                f"Suggestions: {sort_suggestions if sort_suggestions else 'no close matches'}. "
                f"Available columns ({len(allowed_cols)}): {allowed_cols}"
            )
        df = df.sort_values(by=sort_by, ascending=(sort_order == "asc"))

    if limit:
        df = df.head(limit)

    # Step 7: Field selection — ``symbol`` is always kept
    if fields is not None:
        keep = ["symbol"] + [f for f in fields if f != "symbol"]
        missing = [f for f in keep if f not in df.columns]
        if missing:
            raise ValueError(
                f"Unknown field(s) in 'fields': {missing}. "
                f"Available columns ({len(allowed_cols)}): {allowed_cols}"
            )
        df = df[keep]

    if df.empty:
        raise EmptyDataError(
            provider="fmp",
            message="Screener returned no rows after applying all filters.",
        )

    prov = Provenance(source="fmp_screener", params=params.model_dump(exclude_none=True))
    return SCREENER_OUTPUT.build_table_result(
        df,
        provenance=prov,
        params=params.model_dump(exclude_none=True),
    )


__all__ = [
    "DEFAULT_ENRICHMENT_CONCURRENCY",
    "execute",
]
