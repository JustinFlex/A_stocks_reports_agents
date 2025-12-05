from __future__ import annotations

from datetime import date

import math

from astock_report.domain.models.financials import (
    FinancialDataset,
    FinancialStatement,
)
from astock_report.domain.services.calculations import GrowthCalculator


def _mk_is(ticker: str, y: int, revenue: float, net_income: float) -> FinancialStatement:
    return FinancialStatement(
        ticker=ticker,
        period=date(y, 12, 31),
        statement_type="IS",
        metrics={"revenue": revenue, "net_income": net_income},
    )


def test_growth_cagr_and_yoy_basic():
    # Build 4 years of 10% growth in revenue and net income
    t = "TEST"
    is_list = [
        _mk_is(t, 2019, 100.0, 10.0),
        _mk_is(t, 2020, 110.0, 11.0),
        _mk_is(t, 2021, 121.0, 12.1),
        _mk_is(t, 2022, 133.1, 13.31),
    ]
    # Add minimal BS/CF to pass is_complete
    bs = FinancialStatement(ticker=t, period=date(2022, 12, 31), statement_type="BS", metrics={})
    cf = FinancialStatement(ticker=t, period=date(2022, 12, 31), statement_type="CF", metrics={})
    ds = FinancialDataset(ticker=t, income_statements=is_list, balance_sheets=[bs], cash_flows=[cf])

    calc = GrowthCalculator()
    gc = calc.calculate(ds)

    assert math.isfinite(gc.metrics["revenue_cagr"]) and math.isfinite(gc.metrics["net_income_cagr"])  # type: ignore[index]
    assert abs(gc.metrics["revenue_cagr"] - 0.10) < 1e-4  # type: ignore[index]
    assert abs(gc.metrics["net_income_cagr"] - 0.10) < 1e-4  # type: ignore[index]
    # Last YoY also ~10%
    assert abs(gc.metrics["revenue_yoy"] - 0.10) < 1e-4  # type: ignore[index]
    assert abs(gc.metrics["net_income_yoy"] - 0.10) < 1e-4  # type: ignore[index]

