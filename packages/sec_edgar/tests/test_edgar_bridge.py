"""Unit tests for the edgartools bridge (_edgar.py).

Tests the internal helpers that the connector-layer mocks skip over:
_cik_to_int, _melt_statement, column rename correctness, and the
error-handling paths inside _sync_get_financials / _sync_get_insider_transactions
/ _sync_get_holdings_13f. No network calls — edgartools is mocked at the
``edgar`` module level.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from parsimony.errors import EmptyDataError, ParseError

import parsimony_sec_edgar._edgar as bridge

# ---------------------------------------------------------------------------
# _cik_to_int
# ---------------------------------------------------------------------------


def test_cik_to_int_strips_leading_zeros() -> None:
    assert bridge._cik_to_int("0000320193") == 320193


def test_cik_to_int_all_zeros_returns_zero() -> None:
    assert bridge._cik_to_int("0000000000") == 0


def test_cik_to_int_bare_number() -> None:
    assert bridge._cik_to_int("1067983") == 1067983


# ---------------------------------------------------------------------------
# _melt_statement
# ---------------------------------------------------------------------------


def _wide_df() -> pd.DataFrame:
    """Realistic Statement.to_dataframe() output shape.

    Mirrors the live edgartools schema (v5.x): namespaced concepts, a clutch of
    metadata columns beyond concept/label (standard_concept, balance, weight,
    preferred_sign, parent_concept), period columns annotated with the fiscal
    period ("(FY)"), and a *dimensional breakdown* row (the iPhone slice of Net
    sales, dimension=True) that the melt must drop so each concept stays at the
    consolidated top line.
    """
    return pd.DataFrame(
        {
            "concept": ["us-gaap_Revenues", "us-gaap_Revenues", "us-gaap_NetIncomeLoss"],
            "label": ["Net sales", "iPhone", "Net Income"],
            "standard_concept": ["Revenue", None, "NetIncomeLoss"],
            "level": [4, 4, 4],
            "abstract": [False, False, False],
            "dimension": [False, True, False],  # row 1 is a product breakdown → must be dropped
            "is_breakdown": [False, False, False],
            "balance": ["credit", "credit", "credit"],
            "weight": [1.0, 1.0, 1.0],  # float metadata: would survive dropna if it leaked into periods
            "preferred_sign": [1.0, 1.0, 1.0],
            "parent_concept": ["us-gaap_GrossProfit", "us-gaap_GrossProfit", None],
            "2023-09-30 (FY)": [383285000000, 200583000000, 96995000000],
            "2022-09-24 (FY)": [394328000000, 205489000000, 99803000000],
        }
    )


def test_melt_produces_tidy_long_format() -> None:
    result = bridge._melt_statement(_wide_df())
    assert list(result.columns) == ["concept", "label", "period", "value"]
    # 2 consolidated concepts × 2 periods (the dimensional iPhone row is dropped).
    assert len(result) == 4


def test_melt_drops_non_period_metadata_cols() -> None:
    result = bridge._melt_statement(_wide_df())
    for leaked in (
        "level",
        "abstract",
        "dimension",
        "is_breakdown",
        "standard_concept",
        "balance",
        "weight",
        "preferred_sign",
        "parent_concept",
    ):
        assert leaked not in result.columns


def test_melt_no_metadata_leaks_into_period_axis() -> None:
    # Regression: a metadata-column blacklist let text columns (standard_concept,
    # balance) and float columns (weight=1.0) melt into the period axis — and the
    # float ones survived dropna. The period axis must contain only real dates.
    result = bridge._melt_statement(_wide_df())
    assert set(result["period"]) == {"2023-09-30", "2022-09-24"}
    assert (result["value"] != 1.0).all(), "weight/preferred_sign metadata leaked as values"


def test_melt_drops_dimensional_breakdown_rows() -> None:
    # The iPhone slice (dimension=True) must not appear; Net sales stays consolidated.
    result = bridge._melt_statement(_wide_df())
    assert "iPhone" not in set(result["label"])
    net_sales = result[result["concept"] == "us-gaap_Revenues"]
    assert set(net_sales["label"]) == {"Net sales"}
    assert len(net_sales) == 2  # one consolidated row per period


def test_melt_normalizes_period_to_bare_date() -> None:
    # The "(FY)" fiscal annotation is stripped; the period is the bare end date.
    result = bridge._melt_statement(_wide_df())
    assert all(len(p) == 10 and p.count("-") == 2 for p in result["period"])


def test_melt_preserves_label() -> None:
    result = bridge._melt_statement(_wide_df())
    revenues = result[result["concept"] == "us-gaap_Revenues"]
    assert set(revenues["label"]) == {"Net sales"}


def test_melt_drops_nan_values() -> None:
    df = _wide_df()
    df.loc[0, "2022-09-24 (FY)"] = None  # one cell of the consolidated Net sales row goes missing
    result = bridge._melt_statement(df)
    # 2 consolidated concepts × 2 periods - 1 NaN = 3 rows
    assert len(result) == 3


def test_melt_single_period() -> None:
    df = pd.DataFrame(
        {
            "concept": ["Assets"],
            "label": ["Total Assets"],
            "2023-12-31": [500000000],
        }
    )
    result = bridge._melt_statement(df)
    assert len(result) == 1
    assert result.iloc[0]["period"] == "2023-12-31"
    assert result.iloc[0]["value"] == 500000000


# ---------------------------------------------------------------------------
# _sync_get_financials — error paths (edgartools mocked)
# ---------------------------------------------------------------------------


def _mock_edgar_company(financials=None):
    company = MagicMock()
    company.get_financials.return_value = financials
    return company


def test_sync_get_financials_no_financials_raises_empty() -> None:
    company = _mock_edgar_company(financials=None)
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError) as exc:
            bridge._sync_get_financials("0000320193", "income_statement", "Test Co test@ex.com")
    assert "0000320193" in str(exc.value)


def test_sync_get_financials_no_statement_raises_empty() -> None:
    financials = MagicMock()
    financials.income_statement.return_value = None
    company = _mock_edgar_company(financials=financials)
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError):
            bridge._sync_get_financials("0000320193", "income_statement", "Test Co test@ex.com")


def test_sync_get_financials_to_dataframe_exception_raises_parse() -> None:
    stmt = MagicMock()
    stmt.to_dataframe.side_effect = RuntimeError("bad xbrl")
    financials = MagicMock()
    financials.income_statement.return_value = stmt
    company = _mock_edgar_company(financials=financials)
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(ParseError) as exc:
            bridge._sync_get_financials("0000320193", "income_statement", "Test Co test@ex.com")
    assert "bad xbrl" in str(exc.value)


def test_sync_get_financials_empty_dataframe_raises_empty() -> None:
    stmt = MagicMock()
    stmt.to_dataframe.return_value = pd.DataFrame()
    financials = MagicMock()
    financials.income_statement.return_value = stmt
    company = _mock_edgar_company(financials=financials)
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError):
            bridge._sync_get_financials("0000320193", "income_statement", "Test Co test@ex.com")


def test_sync_get_financials_happy_path_returns_tidy_frame() -> None:
    stmt = MagicMock()
    stmt.to_dataframe.return_value = _wide_df()
    financials = MagicMock()
    financials.cashflow_statement.return_value = stmt
    company = _mock_edgar_company(financials=financials)
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        result = bridge._sync_get_financials("0000320193", "cashflow_statement", "Test Co test@ex.com")
    assert list(result.columns) == ["concept", "label", "period", "value"]
    assert len(result) == 4


# ---------------------------------------------------------------------------
# _sync_get_insider_transactions — error paths
# ---------------------------------------------------------------------------


def test_sync_get_insider_transactions_no_filings_raises_empty() -> None:
    company = MagicMock()
    company.get_filings.return_value = iter([])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError):
            bridge._sync_get_insider_transactions("0000320193", 20, "Test Co test@ex.com")


def test_sync_get_insider_transactions_broken_filings_skipped() -> None:
    bad_filing = MagicMock()
    bad_filing.obj.side_effect = RuntimeError("parse error")
    company = MagicMock()
    company.get_filings.return_value = iter([bad_filing])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError):
            bridge._sync_get_insider_transactions("0000320193", 20, "Test Co test@ex.com")


def test_sync_get_insider_transactions_renames_columns() -> None:
    raw_df = pd.DataFrame(
        {
            "Transaction Type": ["Sale"],
            "Code": ["S"],
            "Shares": [100000.0],
            "Price": [182.5],
            "Value": [18250000.0],
            "Date": pd.to_datetime(["2024-01-15"]),
            "Issuer": ["Apple Inc."],
            "Ticker": ["AAPL"],
            "Insider": ["Cook Timothy D"],
            "Position": ["CEO"],
            "Remaining Shares": [3500000.0],
        }
    )
    form4 = MagicMock()
    form4.to_dataframe.return_value = raw_df
    filing = MagicMock()
    filing.obj.return_value = form4
    company = MagicMock()
    company.get_filings.return_value = iter([filing])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        result = bridge._sync_get_insider_transactions("0000320193", 20, "Test Co test@ex.com")
    assert "transaction_type" in result.columns
    assert "remaining_shares" in result.columns
    assert "issuer" in result.columns
    assert result.iloc[0]["ticker"] == "AAPL"


# ---------------------------------------------------------------------------
# _sync_get_holdings_13f — error paths
# ---------------------------------------------------------------------------


def test_sync_get_holdings_13f_no_filings_raises_empty() -> None:
    company = MagicMock()
    company.get_filings.return_value = iter([])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError):
            bridge._sync_get_holdings_13f("0001067983", "Test Co test@ex.com")


def test_sync_get_holdings_13f_parse_failure_raises_parse() -> None:
    filing = MagicMock()
    filing.obj.side_effect = RuntimeError("bad 13f xml")
    company = MagicMock()
    company.get_filings.return_value = iter([filing])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(ParseError):
            bridge._sync_get_holdings_13f("0001067983", "Test Co test@ex.com")


def test_sync_get_holdings_13f_empty_holdings_raises_empty() -> None:
    thirteenf = MagicMock()
    thirteenf.holdings = pd.DataFrame()
    filing = MagicMock()
    filing.obj.return_value = thirteenf
    company = MagicMock()
    company.get_filings.return_value = iter([filing])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        with pytest.raises(EmptyDataError):
            bridge._sync_get_holdings_13f("0001067983", "Test Co test@ex.com")


def test_sync_get_holdings_13f_renames_columns() -> None:
    raw_holdings = pd.DataFrame(
        {
            "Issuer": ["Apple Inc."],
            "Class": ["COM"],
            "Cusip": ["037833100"],
            "Ticker": ["AAPL"],
            "Type": ["SH"],
            "PutCall": [None],
            "SharesPrnAmount": [5000000.0],
            "Value": [912500000.0],
            "SoleVoting": [5000000.0],
            "SharedVoting": [0.0],
            "NonVoting": [0.0],
        }
    )
    thirteenf = MagicMock()
    thirteenf.holdings = raw_holdings
    filing = MagicMock()
    filing.obj.return_value = thirteenf
    company = MagicMock()
    company.get_filings.return_value = iter([filing])
    with patch("parsimony_sec_edgar._edgar.edgar") as mock_edgar:
        mock_edgar.Company.return_value = company
        result = bridge._sync_get_holdings_13f("0001067983", "Test Co test@ex.com")
    assert "cusip" in result.columns
    assert "security_class" in result.columns
    assert "shares" in result.columns
    assert result.iloc[0]["ticker"] == "AAPL"
    assert result.iloc[0]["cusip"] == "037833100"
