from __future__ import annotations

from datetime import date

import math

from astock_report.domain.models.financials import FinancialDataset, FinancialStatement
from astock_report.domain.services.calculations import RatioCalculator, ValuationEngine


def make_dataset() -> FinancialDataset:
    t = "TEST"
    # Older period (t-1)
    is_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="IS",
        metrics={
            "revenue": 1000.0,
            "ebitda": 240.0,
            "net_income": 120.0,
        },
    )
    bs_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="BS",
        metrics={
            "cash_and_equivalents": 100.0,
            "short_term_debt": 150.0,
            "long_term_debt": 250.0,
            "shares_outstanding": 100.0,
        },
    )
    cf_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="CF",
        metrics={
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
            "ebitda": 264.0,
            "net_income": 132.0,
        },
    )
    bs_new = FinancialStatement(
        ticker=t,
        period=date(2022, 12, 31),
        statement_type="BS",
        metrics={
            "cash_and_equivalents": 120.0,
            "short_term_debt": 160.0,
            "long_term_debt": 260.0,
            "shares_outstanding": 100.0,
            "price": 15.0,
            # Provide DCF assumptions to demonstrate override
            "wacc": 0.10,
            "g": 0.08,
            "terminal_growth": 0.03,
            "forecast_years": 5,
        },
    )
    cf_new = FinancialStatement(
        ticker=t,
        period=date(2022, 12, 31),
        statement_type="CF",
        metrics={
            "free_cash_flow": 132.0,
        },
    )
    return FinancialDataset(
        ticker=t,
        income_statements=[is_old, is_new],
        balance_sheets=[bs_old, bs_new],
        cash_flows=[cf_old, cf_new],
    )


def test_valuation_engine_methods():
    ds = make_dataset()
    rc = RatioCalculator()
    rs = rc.calculate(ds)

    ve = ValuationEngine()
    vb = ve.run(ds, rs)

    assert vb.valuation_methods
    assert "dcf" in vb.valuation_methods
    assert "pe_band" in vb.valuation_methods
    assert "ev_ebitda" in vb.valuation_methods

    dcf_fv = vb.valuation_methods["dcf"]["fair_value"]
    # Roughly around ~20 per share for the configured inputs
    assert 18.0 < dcf_fv < 22.0

    pe_fv = vb.valuation_methods["pe_band"]["fair_value"]
    assert 19.0 < pe_fv < 21.0

    ev_fv = vb.valuation_methods["ev_ebitda"]["fair_value"]
    assert 19.0 < ev_fv < 22.0

