"""Declarative output schemas for the FMP connectors.

One :class:`OutputSpec` per DataFrame-returning connector that projects a
shaped frame out of FMP's raw JSON. Column names and order are the contract with
the catalog / tool surface — renaming or re-ordering them is a breaking change.

Role conventions:

* ``symbol`` is the entity identity on every equity verb, so it is a namespaced
  ``KEY`` (namespace ``fmp_symbols``), never ``METADATA``. ``OutputSpec`` allows
  at most one ``KEY`` per schema.
* Columns are declared **only** when the live FMP payload actually carries them
  (verified 2026-06-04). Declaring a column the payload can't populate would
  surface a constant-empty column to the agent.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputSpec

_NS = "fmp_symbols"


SEARCH_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="currency"),
        Column(name="exchangeFullName"),
        Column(name="exchange"),
    ]
)

COMPANY_PROFILE_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="companyName"),
        Column(name="price"),
        Column(name="marketCap"),
        Column(name="beta"),
        Column(name="exchange"),
        Column(name="exchangeFullName"),
        Column(name="currency"),
        Column(name="sector"),
        Column(name="industry"),
        Column(name="country"),
        Column(name="fullTimeEmployees"),
        Column(name="ceo"),
        Column(name="description"),
        Column(name="website"),
        Column(name="ipoDate"),
        Column(name="isEtf"),
        Column(name="isActivelyTrading"),
        Column(name="isAdr"),
        Column(name="isFund"),
    ]
)

INCOME_STATEMENT_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date"),
        Column(name="reportedCurrency", role=ColumnRole.METADATA),
        Column(name="revenue"),
        Column(name="costOfRevenue"),
        Column(name="grossProfit"),
        Column(name="operatingExpenses"),
        Column(name="operatingIncome"),
        Column(name="ebitda"),
        Column(name="netIncome"),
        Column(name="eps"),
        Column(name="epsDiluted"),
    ]
)

BALANCE_SHEET_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date"),
        Column(name="totalAssets"),
        Column(name="totalLiabilities"),
        Column(name="totalStockholdersEquity"),
        Column(name="totalDebt"),
        Column(name="netDebt"),
        Column(name="cashAndCashEquivalents"),
        Column(name="*"),
    ]
)

CASH_FLOW_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date"),
        Column(name="reportedCurrency", role=ColumnRole.METADATA),
        Column(name="netIncome"),
        Column(name="operatingCashFlow"),
        Column(name="capitalExpenditure"),
        Column(name="freeCashFlow"),
        Column(name="netCashProvidedByOperatingActivities"),
        Column(name="netCashProvidedByInvestingActivities"),
        Column(name="netCashProvidedByFinancingActivities"),
        Column(name="netChangeInCash"),
        Column(name="*"),
    ]
)

HISTORICAL_PRICES_OUTPUT = OutputSpec(
    columns=[
        # Not normalized so intraday frequencies (1min..4hour) keep their
        # time component.
        Column(name="date"),
        Column(name="open"),
        Column(name="high"),
        Column(name="low"),
        Column(name="close"),
        Column(name="volume"),
        Column(name="change"),
        Column(name="changePercent"),
        Column(name="vwap"),
    ]
)

# batch-quote payload (verified live): symbol, name, price, change,
# changePercentage, dayLow, dayHigh, yearLow, yearHigh, marketCap, volume,
# priceAvg50, priceAvg200, exchange, open, previousClose, timestamp. It does NOT
# carry avgVolume / pe / eps / changesPercentage — those are dropped here so the
# schema does not declare columns the payload can't populate.
STOCK_QUOTE_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="price"),
        Column(name="changePercentage"),
        Column(name="change"),
        Column(name="dayLow"),
        Column(name="dayHigh"),
        Column(name="yearLow"),
        Column(name="yearHigh"),
        Column(name="marketCap"),
        Column(name="volume"),
        Column(name="priceAvg50"),
        Column(name="priceAvg200"),
        Column(name="exchange"),
        Column(name="open"),
        Column(name="previousClose"),
    ]
)

PEERS_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="companyName"),
        Column(name="price"),
        Column(name="mktCap"),
    ]
)

NEWS_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="publishedDate"),
        Column(name="title"),
        Column(name="text"),
        Column(name="url"),
        Column(name="site"),
        Column(name="image"),
    ]
)

INSIDER_TRADES_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="filingDate"),
        Column(name="transactionDate"),
        Column(name="reportingName"),
        Column(name="typeOfOwner"),
        Column(name="transactionType"),
        Column(name="acquisitionOrDisposition"),
        Column(name="securitiesTransacted"),
        Column(name="price"),
        Column(name="securitiesOwned"),
        Column(name="formType"),
        Column(name="url"),
    ]
)

INSTITUTIONAL_POSITIONS_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date"),
        Column(name="investorsHolding"),
        Column(name="investorsHoldingChange"),
        Column(name="numberOf13Fshares"),
        Column(name="numberOf13FsharesChange"),
        Column(name="totalInvested"),
        Column(name="totalInvestedChange"),
        Column(name="ownershipPercent"),
        Column(name="ownershipPercentChange"),
        Column(name="newPositions"),
        Column(name="closedPositions"),
        Column(name="increasedPositions"),
        Column(name="reducedPositions"),
        Column(name="putCallRatio"),
    ]
)

EARNINGS_TRANSCRIPT_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="year"),
        Column(name="period"),
        Column(name="date"),
        Column(name="content"),
    ]
)

ANALYST_ESTIMATES_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="date"),
        Column(name="revenueLow"),
        Column(name="revenueAvg"),
        Column(name="revenueHigh"),
        Column(name="ebitdaLow"),
        Column(name="ebitdaAvg"),
        Column(name="ebitdaHigh"),
        Column(name="netIncomeLow"),
        Column(name="netIncomeAvg"),
        Column(name="netIncomeHigh"),
        Column(name="epsLow"),
        Column(name="epsAvg"),
        Column(name="epsHigh"),
        Column(name="numAnalystsRevenue"),
        Column(name="numAnalystsEps"),
    ]
)

INDEX_CONSTITUENTS_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="sector"),
        Column(name="subSector"),
        Column(name="headQuarter"),
        Column(name="dateFirstAdded"),
        Column(name="cik"),
        Column(name="founded"),
    ]
)

MARKET_MOVERS_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="name"),
        Column(name="price"),
        Column(name="change"),
        Column(name="changesPercentage"),
        Column(name="exchange"),
    ]
)

SCREENER_OUTPUT = OutputSpec(
    columns=[
        Column(name="symbol", role=ColumnRole.KEY, namespace=_NS),
        Column(name="companyName"),
        Column(name="sector"),
        Column(name="industry"),
        Column(name="country"),
        Column(name="exchange"),
        Column(name="marketCap"),
        Column(name="price"),
        Column(name="beta"),
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
