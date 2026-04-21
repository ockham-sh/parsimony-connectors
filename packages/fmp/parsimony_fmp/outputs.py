"""Declarative output schemas for the FMP connectors.

One :class:`OutputConfig` per connector that projects a shaped DataFrame
out of FMP's raw JSON. Columns declared here are the contract with the
MCP tool catalog — renaming or re-ordering them is a breaking change.
"""

from __future__ import annotations

from parsimony.result import Column, ColumnRole, OutputConfig

SEARCH_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="name"),
        Column(name="currency"),
        Column(name="exchangeFullName"),
        Column(name="exchange"),
    ]
)

COMPANY_PROFILE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
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
        Column(name="date", dtype="datetime"),
        Column(name="symbol", role=ColumnRole.METADATA),
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
        Column(name="date", dtype="datetime"),
        Column(name="symbol", role=ColumnRole.METADATA),
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
        Column(name="date", dtype="datetime"),
        Column(name="symbol", role=ColumnRole.METADATA),
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
        Column(name="date", dtype="date"),
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

STOCK_QUOTE_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
        Column(name="name"),
        Column(name="price", dtype="numeric"),
        Column(name="changesPercentage", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="dayLow", dtype="numeric"),
        Column(name="dayHigh", dtype="numeric"),
        Column(name="yearLow", dtype="numeric"),
        Column(name="yearHigh", dtype="numeric"),
        Column(name="marketCap", dtype="numeric"),
        Column(name="volume", dtype="numeric"),
        Column(name="avgVolume", dtype="numeric"),
        Column(name="pe", dtype="numeric"),
        Column(name="eps", dtype="numeric"),
        Column(name="priceAvg50", dtype="numeric"),
        Column(name="priceAvg200", dtype="numeric"),
        Column(name="exchange"),
        Column(name="open", dtype="numeric"),
        Column(name="previousClose", dtype="numeric"),
    ]
)

PEERS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="companyName"),
        Column(name="price", dtype="numeric"),
        Column(name="mktCap", dtype="numeric"),
    ]
)

NEWS_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
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
        Column(name="symbol", role=ColumnRole.METADATA),
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
        Column(name="symbol", role=ColumnRole.METADATA),
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
        Column(name="symbol", role=ColumnRole.METADATA),
        Column(name="year", dtype="numeric"),
        Column(name="period"),
        Column(name="date", dtype="date"),
        Column(name="content"),
    ]
)

ANALYST_ESTIMATES_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
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
        Column(name="symbol"),
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
        Column(name="symbol"),
        Column(name="name"),
        Column(name="price", dtype="numeric"),
        Column(name="change", dtype="numeric"),
        Column(name="changesPercentage", dtype="numeric"),
        Column(name="exchange"),
    ]
)

SCREENER_OUTPUT = OutputConfig(
    columns=[
        Column(name="symbol"),
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
