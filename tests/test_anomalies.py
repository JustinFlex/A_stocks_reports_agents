from __future__ import annotations

from datetime import date

from astock_report.domain.models.financials import FinancialDataset, FinancialStatement
from astock_report.domain.services.calculations import AnomalyDetector, RatioCalculator


def make_dataset() -> FinancialDataset:
    t = "TEST"
    is_old = FinancialStatement(
        ticker=t,
        period=date(2021, 12, 31),
        statement_type="IS",
        metrics={"revenue": 100.0, "net_income": 10.0},
    )
    is_new = FinancialStatement(
        ticker=t,
        period=date(2022, 12, 31),
        statement_type="IS",
        metrics={"revenue": 150.0, "net_income": -5.0},
    )
    bs = FinancialStatement(ticker=t, period=date(2022, 12, 31), statement_type="BS", metrics={"total_equity": 50.0})
    cf = FinancialStatement(ticker=t, period=date(2022, 12, 31), statement_type="CF", metrics={"operating_cash_flow": 1.0})
    return FinancialDataset(ticker=t, income_statements=[is_old, is_new], balance_sheets=[bs], cash_flows=[cf])


def test_anomaly_detector_flags_growth_and_profitability():
    ds = make_dataset()
    rc = RatioCalculator()
    ratios = rc.calculate(ds)

    detector = AnomalyDetector()
    flags = detector.detect(ds, ratios)

    assert flags["growth"]  # revenue jump >30%
    assert any("revenue" in msg for msg in flags["growth"])
    assert flags["profitability"]  # net margin negative
    assert any("Net margin negative" in msg for msg in flags["profitability"])
