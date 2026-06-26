"""Declarative output schemas for the FMP connectors.

One :class:`OutputConfig` per DataFrame-returning connector that projects a
shaped frame out of FMP's raw JSON. Column names and order are the contract with
the catalog / tool surface — renaming or re-ordering them is a breaking change.

Role conventions:

* ``symbol`` is the entity identity on every equity verb, so it is a namespaced
  ``KEY`` (namespace ``fmp_symbols``), never ``METADATA``. ``OutputConfig`` allows
  at most one ``KEY`` per schema.
* Columns are declared **only** when the live FMP payload actually carries them
  (verified 2026-06-04). Declaring a column the payload can't populate would
  surface a constant-empty column to the agent.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

_NS = "fmp_symbols"


SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="currency"),
        Column(name="exchangeFullName"),
        Column(name="exchange"),
    ]
)

COMPANY_PROFILE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="companyName"),
        Column(name="price", dtype="numeric"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="beta", dtype="numeric"),
        Column(name="exchange"),
        Column(name="exchangeFullName"),
        Column(name="currency"),
        Column(name="sector"),
        Column(name="industry"),
        Column(name="country"),
        Column(name="fullTimeEmployees", dtype="numeric"),
        Column(name="ceo"),
        Column(name="description"),
        Column(name="website"),
        Column(name="ipoDate"),
        Column(name="isEtf", dtype="bool"),
        Column(name="isActivelyTrading", dtype="bool"),
        Column(name="isAdr", dtype="bool"),
        Column(name="isFund", dtype="bool"),
    ]
)

INCOME_STATEMENT_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date", dtype="datetime"),
        Column(name="reportedCurrency", role=ColumnRole.METADATA),
        Column(name="revenue", dtype="numeric"),
        Column(name="costOfRevenue", dtype="numeric"),
        Column(name="grossProfit", dtype="numeric"),
        Column(name="operatingExpenses", dtype="numeric"),
        Column(name="operatingIncome", dtype="numeric"),
        Column(name="ebitda", dtype="numeric"),
        Column(name="netIncome", dtype="numeric"),
        Column(name="eps", dtype="numeric"),
        Column(name="epsDiluted", dtype="numeric"),
    ]
)

BALANCE_SHEET_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date", dtype="datetime"),
        Column(name="totalAssets", dtype="numeric"),
        Column(name="totalLiabilities", dtype="numeric"),
        Column(name="totalStockholdersEquity", dtype="numeric"),
        Column(name="totalDebt", dtype="numeric"),
        Column(name="netDebt", dtype="numeric"),
        Column(name="cashAndCashEquivalents", dtype="numeric"),
        Column(name="*"),
    ]
)

CASH_FLOW_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date", dtype="datetime"),
        Column(name="reportedCurrency", role=ColumnRole.METADATA),
        Column(name="netIncome", dtype="numeric"),
        Column(name="operatingCashFlow", dtype="numeric"),
        Column(name="capitalExpenditure", dtype="numeric"),
        Column(name="freeCashFlow", dtype="numeric"),
        Column(name="netCashProvidedByOperatingActivities", dtype="numeric"),
        Column(name="netCashProvidedByInvestingActivities", dtype="numeric"),
        Column(name="netCashProvidedByFinancingActivities", dtype="numeric"),
        Column(name="netChangeInCash", dtype="numeric"),
        Column(name="*"),
    ]
)

HISTORICAL_PRICES_OUTPUT = OutputConfig(
    columns=[
        # `datetime` (not `date`) so intraday frequencies (1min..4hour) keep their
        # time component. `date` runs `dt.normalize()`, which would zero out the
        # time on every row regardless of frequency.
        Column(name="date", dtype="datetime"),
        Column(name="open", dtype="numeric"),
        Column(name="high", dtype="numeric"),
        Column(name="low", dtype="numeric"),
        Column(name="close", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="changePercent", dtype="numeric"),
        Column(name="vwap", dtype="numeric"),
    ]
)

# batch-quote payload (verified live): symbol, name, price, change,
# changePercentage, dayLow, dayHigh, yearLow, yearHigh, marketCap, volume,
# priceAvg50, priceAvg200, exchange, open, previousClose, timestamp. It does NOT
# carry avgVolume / pe / eps / changesPercentage — those are dropped here so the
# schema does not declare columns the payload can't populate.
STOCK_QUOTE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="price", dtype="numeric"),
        Column(name="changePercentage", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="dayLow", dtype="numeric"),
        Column(name="dayHigh", dtype="numeric"),
        Column(name="yearLow", dtype="numeric"),
        Column(name="yearHigh", dtype="numeric"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="priceAvg50", dtype="numeric"),
        Column(name="priceAvg200", dtype="numeric"),
        Column(name="exchange"),
        Column(name="open", dtype="numeric"),
        Column(name="previousClose", dtype="numeric"),
    ]
)

PEERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="companyName"),
        Column(name="price", dtype="numeric"),
        Column(name="mktCap", dtype="numeric"),
    ]
)

NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="publishedDate", dtype="datetime"),
        Column(name="title"),
        Column(name="text"),
        Column(name="url"),
        Column(name="site"),
        Column(name="image"),
    ]
)

INSIDER_TRADES_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="filingDate", dtype="datetime"),
        Column(name="transactionDate", dtype="datetime"),
        Column(name="reportingName"),
        Column(name="typeOfOwner"),
        Column(name="transactionType"),
        Column(name="acquisitionOrDisposition"),
        Column(name="securitiesTransacted", dtype="numeric"),
        Column(name="price", dtype="numeric"),
        Column(name="securitiesOwned", dtype="numeric"),
        Column(name="formType"),
        Column(name="url"),
    ]
)

INSTITUTIONAL_POSITIONS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date", dtype="date"),
        Column(name="investorsHolding", dtype="numeric"),
        Column(name="investorsHoldingChange", dtype="numeric"),
        Column(name="numberOf13Fshares", dtype="numeric"),
        Column(name="numberOf13FsharesChange", dtype="numeric"),
        Column(name="totalInvested", dtype="numeric"),
        Column(name="totalInvestedChange", dtype="numeric"),
        Column(name="ownershipPercent", dtype="numeric"),
        Column(name="ownershipPercentChange", dtype="numeric"),
        Column(name="newPositions", dtype="numeric"),
        Column(name="closedPositions", dtype="numeric"),
        Column(name="increasedPositions", dtype="numeric"),
        Column(name="reducedPositions", dtype="numeric"),
        Column(name="putCallRatio", dtype="numeric"),
    ]
)

EARNINGS_TRANSCRIPT_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="year", dtype="numeric"),
        Column(name="period"),
        Column(name="date", dtype="date"),
        Column(name="content"),
    ]
)

ANALYST_ESTIMATES_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date", dtype="date"),
        Column(name="revenueLow", dtype="numeric"),
        Column(name="revenueAvg", dtype="numeric"),
        Column(name="revenueHigh", dtype="numeric"),
        Column(name="ebitdaLow", dtype="numeric"),
        Column(name="ebitdaAvg", dtype="numeric"),
        Column(name="ebitdaHigh", dtype="numeric"),
        Column(name="netIncomeLow", dtype="numeric"),
        Column(name="netIncomeAvg", dtype="numeric"),
        Column(name="netIncomeHigh", dtype="numeric"),
        Column(name="epsLow", dtype="numeric"),
        Column(name="epsAvg", dtype="numeric"),
        Column(name="epsHigh", dtype="numeric"),
        Column(name="numAnalystsRevenue", dtype="numeric"),
        Column(name="numAnalystsEps", dtype="numeric"),
    ]
)

INDEX_CONSTITUENTS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="sector"),
        Column(name="subSector"),
        Column(name="headQuarter"),
        Column(name="dateFirstAdded", dtype="date"),
        Column(name="cik"),
        Column(name="founded"),
    ]
)

MARKET_MOVERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="price", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="changesPercentage", dtype="numeric"),
        Column(name="exchange"),
    ]
)

SCREENER_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="companyName"),
        Column(name="sector"),
        Column(name="industry"),
        Column(name="country"),
        Column(name="exchange"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="price", dtype="numeric"),
        Column(name="beta", dtype="numeric"),
        Column(name="*"),
    ]
)


__all__ = [
    "ANALYST_ESTIMATES_OUTPUT",
    "BALANCE_SHEET_OUTPUT",
    "CASH_FLOW_OUTPUT",
    "COMPANY_PROFILE_OUTPUT",
    "EARNINGS_TRANSCRIPT_OUTPUT",
    "HISTORICAL_PRICES_OUTPUT",
    "INCOME_STATEMENT_OUTPUT",
    "INDEX_CONSTITUENTS_OUTPUT",
    "INSIDER_TRADES_OUTPUT",
    "INSTITUTIONAL_POSITIONS_OUTPUT",
    "MARKET_MOVERS_OUTPUT",
    "NEWS_OUTPUT",
    "PEERS_OUTPUT",
    "SCREENER_OUTPUT",
    "SEARCH_OUTPUT",
    "STOCK_QUOTE_OUTPUT",
]
