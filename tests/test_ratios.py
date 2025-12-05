from __future__ import annotations

from datetime import date

import math

from astock_report.domain.models.financials import FinancialDataset, FinancialStatement
from astock_report.domain.services.calculations import RatioCalculator


def make_dataset() -> FinancialDataset:
    t = "TEST"
    # Older period (t-1)
    is_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="IS",
        metrics={
            "revenue": 1000.0,
            "cogs": 600.0,
            "gross_profit": 400.0,
            "operating_income": 200.0,
            "ebit": 200.0,
            "ebitda": 240.0,
            "net_income": 120.0,
            "interest_expense": 20.0,
            "rd_expense": 30.0,
            "sga_expense": 50.0,
            "depreciation_amortization": 40.0,
        },
    )
    bs_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="BS",
        metrics={
            "total_assets": 1500.0,
            "total_equity": 800.0,
            "total_liabilities": 700.0,
            "cash_and_equivalents": 100.0,
            "short_term_debt": 150.0,
            "long_term_debt": 250.0,
            "inventory": 200.0,
            "accounts_receivable": 150.0,
            "accounts_payable": 100.0,
            "current_assets": 700.0,
            "current_liabilities": 300.0,
            "shares_outstanding": 100.0,
        },
    )
    cf_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="CF",
        metrics={
            "operating_cash_flow": 180.0,
            "capital_expenditures": 60.0,
            "free_cash_flow": 120.0,
        },
    )
    # Latest period
    is_new = FinancialStatement(
        ticker=t,
        period=date(2022, 12, 31),
        statement_type="IS",
        metrics={
            "revenue": 1100.0,
            "cogs": 660.0,
            "gross_profit": 440.0,
            "operating_income": 220.0,
            "ebit": 220.0,
            "ebitda": 264.0,
            "net_income": 132.0,
            "interest_expense": 22.0,
            "rd_expense": 33.0,
            "sga_expense": 55.0,
            "depreciation_amortization": 44.0,
        },
    )
    bs_new = FinancialStatement(
        ticker=t,
        period=date(2022, 12, 31),
        statement_type="BS",
        metrics={
            "total_assets": 1600.0,
            "total_equity": 820.0,
            "total_liabilities": 780.0,
            "cash_and_equivalents": 120.0,
            "short_term_debt": 160.0,
            "long_term_debt": 260.0,
            "inventory": 210.0,
            "accounts_receivable": 160.0,
            "accounts_payable": 110.0,
            "current_assets": 740.0,
            "current_liabilities": 320.0,
            "shares_outstanding": 100.0,
            "price": 15.0,
            "dividend_per_share": 0.5,
        },
    )
    cf_new = FinancialStatement(
        ticker=t,
        period=date(2022, 12, 31),
        statement_type="CF",
        metrics={
            "operating_cash_flow": 198.0,
            "capital_expenditures": 66.0,
            "free_cash_flow": 132.0,
        },
    )
    return FinancialDataset(
        ticker=t,
        income_statements=[is_old, is_new],
        balance_sheets=[bs_old, bs_new],
        cash_flows=[cf_old, cf_new],
    )


def test_core_ratios_and_valuation_bases():
    ds = make_dataset()
    rc = RatioCalculator()
    rs = rc.calculate(ds)
    r = rs.ratios

    def close(a: float, b: float, tol: float = 1e-6) -> bool:
        return math.isfinite(a) and abs(a - b) < tol

    # Margins
    assert close(r["gross_margin"], 0.4)
    assert close(r["operating_margin"], 0.2)
    assert close(r["net_margin"], 0.12)
    assert close(r["ebitda_margin"], 0.24)

    # Returns
    assert close(r["roe"], 132.0 / 810.0)
    assert close(r["roa"], 132.0 / 1550.0)

    # Liquidity
    assert close(r["current_ratio"], 740.0 / 320.0)
    assert close(r["quick_ratio"], (740.0 - 210.0) / 320.0)

    # Leverage and coverage
    assert close(r["debt_to_equity"], 420.0 / 820.0)
    assert close(r["debt_to_assets"], 420.0 / 1600.0)
    assert close(r["interest_coverage"], 220.0 / 22.0)

    # Efficiency
    assert close(r["asset_turnover"], 1100.0 / 1550.0)
    assert close(r["inventory_turnover"], 660.0 / 205.0)
    assert close(r["receivables_turnover"], 1100.0 / 155.0)
    assert close(r["payables_turnover"], 660.0 / 105.0)

    # Working capital cycle
    dso = 160.0 / 1100.0 * 365.0
    dio = 210.0 / 660.0 * 365.0
    dpo = 110.0 / 660.0 * 365.0
    assert close(r["dso"], dso)
    assert close(r["dio"], dio)
    assert close(r["dpo"], dpo)
    assert close(r["cash_conversion_cycle"], dso + dio - dpo)

    # Cash flow and valuation bases
    assert close(r["fcf_margin"], 132.0 / 1100.0)
    assert close(r["capex_to_sales"], 66.0 / 1100.0)
    assert close(r["ocf_ratio"], 198.0 / 320.0)

    # Valuation ratios
    eps = 132.0 / 100.0
    assert close(r["eps"], eps)
    assert close(r["pe"], 15.0 / eps)
    assert close(r["pb"], 15.0 / (820.0 / 100.0))
    assert close(r["pcf"], 15.0 / (198.0 / 100.0))
    # EV-based
    ev = 15.0 * 100.0 + (420.0 - 120.0)
    assert close(r["ev_over_ebitda"], ev / 264.0)
    assert close(r["ev_over_sales"], ev / 1100.0)

