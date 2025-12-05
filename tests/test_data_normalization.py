"""Unit tests for TuShare -> canonical financial mapping."""
from __future__ import annotations

from datetime import date

import pandas as pd

from astock_report.workflows.nodes.data_load import _normalize_tushare_financials


def test_income_balance_cashflow_mapping_with_aliases():
    ticker = "000001.SZ"

    income_df = pd.DataFrame(
        [
            {
                "end_date": "20231231",
                "revenue": 1000,
                "oper_cost": 600,
                "operate_profit": 120,
                "ebit": 110,
                "ebitda": 150,
                "n_income": 90,
                "int_exp": 5,
                "rd_exp": 20,
                "sell_exp": 30,
            }
        ]
    )

    balance_df = pd.DataFrame(
        [
            {
                "end_date": "20231231",
                "total_assets": 2000,
                "total_hldr_eqy_exc_min_int": 800,
                "total_liab": 1200,
                "money_cap": 300,
                "short_term_borr": 200,
                "long_term_borr": 500,
                "inventories": 100,
                "accounts_receivable": 150,
                "acct_payable": 180,
                "total_cur_assets": 700,
                "total_cur_liab": 400,
                "total_share": 100,
                "dividend": 1.5,
            }
        ]
    )

    cashflow_df = pd.DataFrame(
        [
            {
                "end_date": "20231231",
                "n_cashflow_act": 180,
                "c_paid_acq_const_fiolta": 50,
            }
        ]
    )

    normalized = _normalize_tushare_financials(
        {"income": income_df, "balance": balance_df, "cashflow": cashflow_df}, ticker
    )

    dataset = normalized["dataset"]
    rows = normalized["rows"]

    assert dataset.is_complete()
    assert len(rows) >= 10  # multiple metrics captured

    is_metrics = dataset.income_statements[0].metrics
    bs_metrics = dataset.balance_sheets[0].metrics
    cf_metrics = dataset.cash_flows[0].metrics

    assert is_metrics["revenue"] == 1000
    assert is_metrics["cogs"] == 600
    assert bs_metrics["short_term_debt"] == 200
    assert bs_metrics["long_term_debt"] == 500
    assert bs_metrics["total_liabilities"] == 1200
    assert cf_metrics["operating_cash_flow"] == 180
    assert cf_metrics["capital_expenditures"] == 50
    # Derived free cash flow
    assert cf_metrics["free_cash_flow"] == 130


def test_handles_missing_dates_with_defaults():
    ticker = "600000.SH"
    income_df = pd.DataFrame([{"revenue": 10, "n_income": 2}])
    balance_df = pd.DataFrame([{"total_assets": 20, "total_hldr_eqy_exc_min_int": 8}])
    cashflow_df = pd.DataFrame([{"n_cashflow_act": 5, "c_paid_invest": 1}])

    normalized = _normalize_tushare_financials(
        {"income": income_df, "balance": balance_df, "cashflow": cashflow_df}, ticker
    )

    dataset = normalized["dataset"]
    assert dataset.is_complete()
    # Period should fall back to today when missing
    assert isinstance(dataset.income_statements[0].period, date)


def test_alias_fields_from_tushare_docs():
    ticker = "300001.SZ"
    income_df = pd.DataFrame(
        [
            {
                "f_ann_date": "20240630",
                "total_revenue": 500,
                "total_profit": 80,
                "n_income_attr_p": 70,
                "int_exp": 4,
            }
        ]
    )
    balance_df = pd.DataFrame(
        [
            {
                "end_date": "20240630",
                "total_assets": 900,
                "total_hldr_eqy_inc_min_int": 400,
                "total_liab": 500,
                "money_cap": 120,
                "st_borrow": 90,
                "bond_payable": 180,
                "accounts_receivable": 140,
                "notes_payable": 60,
                "total_cur_assets": 300,
                "total_cur_liab": 200,
                "total_share": 50,
            }
        ]
    )
    cashflow_df = pd.DataFrame(
        [
            {
                "end_date": "20240630",
                "n_cashflow_act": 60,
                "c_pur_fa_olta": 25,
            }
        ]
    )

    normalized = _normalize_tushare_financials(
        {"income": income_df, "balance": balance_df, "cashflow": cashflow_df}, ticker
    )
    ds = normalized["dataset"]
    assert ds.is_complete()
    is_metrics = ds.income_statements[0].metrics
    bs_metrics = ds.balance_sheets[0].metrics
    cf_metrics = ds.cash_flows[0].metrics

    assert is_metrics["revenue"] == 500
    assert is_metrics["operating_income"] == 80
    assert is_metrics["net_income"] == 70
    assert bs_metrics["short_term_debt"] == 90
    assert bs_metrics["long_term_debt"] == 180
    assert bs_metrics["accounts_payable"] == 60
    assert cf_metrics["capital_expenditures"] == 25
    assert cf_metrics["free_cash_flow"] == 35
