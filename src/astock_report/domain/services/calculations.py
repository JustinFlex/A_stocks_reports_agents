"""Domain service layer providing financial calculations.

This module implements:
- Growth metrics using pandas (CAGR, YoY)
- A comprehensive set of financial ratios (25+)
- A valuation engine covering DCF, PE band, EV/EBITDA with simple sensitivity

The implementations are conservative and robust to missing data. Where a value
cannot be computed due to missing or zero denominators, the result is ``float('nan')``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from astock_report.domain.models.financials import (
    FinancialDataset,
    GrowthCurve,
    RatioSummary,
    ValuationBundle,
)


class GrowthCalculator:
    """Compute growth trends such as CAGR and YOY change."""

    def calculate(self, dataset: FinancialDataset) -> GrowthCurve:
        if not dataset.is_complete():
            raise ValueError("Incomplete financial dataset; cannot compute growth curve.")
        statements = _dedup_statements(dataset.income_statements)
        df = _frame_from_statements(statements, keys=["revenue", "net_income"])
        if df.empty:
            metrics = {"revenue_cagr": float("nan"), "net_income_cagr": float("nan")}
            return GrowthCurve(metrics=metrics, commentary=None)

        df = df.sort_values("period").reset_index(drop=True)
        annual_df = _filter_annual(df)
        metrics: Dict[str, float] = {}

        # YoY growth latest (prefer annual, fallback to same-quarter last year)
        for col in ["revenue", "net_income"]:
            metrics[f"{col}_yoy"] = _yoy(df, col)

        # CAGR over the available window
        metrics["revenue_cagr"] = _cagr_from_df(annual_df if not annual_df.empty else df, "revenue")
        metrics["net_income_cagr"] = _cagr_from_df(annual_df if not annual_df.empty else df, "net_income")
        metrics["periods_count"] = float(len(df))
        return GrowthCurve(metrics=metrics, commentary=None)


class AnomalyDetector:
    """Simple anomaly flags for abrupt swings and negative profitability."""

    def detect(self, dataset: FinancialDataset, ratios: RatioSummary) -> Dict[str, List[str]]:
        if not dataset.is_complete():
            raise ValueError("Incomplete financial dataset; cannot detect anomalies.")

        flags: Dict[str, List[str]] = {"growth": [], "profitability": []}

        inc_df = _frame_from_statements(_dedup_statements(dataset.income_statements), keys=["revenue", "net_income"])
        if not inc_df.empty:
            inc_df = inc_df.sort_values("period").tail(12).reset_index(drop=True)
            # Limit anomaly scan to recent periods to avoid flooding the report
            for col in ["revenue", "net_income"]:
                if col not in inc_df:
                    continue
                pct = inc_df[col].pct_change()
                for idx, val in pct.items():
                    if pd.isna(val):
                        continue
                    if abs(val) >= 0.3:
                        try:
                            period = inc_df.loc[idx, "period"]
                        except Exception:
                            period = inc_df.iloc[-1]["period"] if not inc_df.empty else ""
                        flags["growth"].append(f"{col} changed {val:.1%} at {period}")

        net_margin = ratios.ratios.get("net_margin") if ratios and ratios.ratios else None
        if net_margin is not None and not np.isnan(net_margin) and net_margin < 0:
            flags["profitability"].append(f"Net margin negative ({net_margin:.1%})")
        latest_inc, _ = _latest_and_prev(inc_df)
        if latest_inc and "net_income" in latest_inc and not pd.isna(latest_inc["net_income"]) and latest_inc["net_income"] < 0:
            flags["profitability"].append("Latest net income is negative")

        return flags


class RatioCalculator:
    """Derive key financial ratios required by the valuation engine."""

    def calculate(self, dataset: FinancialDataset) -> RatioSummary:
        if not dataset.is_complete():
            raise ValueError("Incomplete financial dataset; cannot compute ratios.")
        inc_stmts = _dedup_statements(dataset.income_statements)
        bs_stmts = _dedup_statements(dataset.balance_sheets)
        cf_stmts = _dedup_statements(dataset.cash_flows)

        inc_df = _frame_from_statements(
            inc_stmts,
            keys=[
                "revenue",
                "cogs",
                "gross_profit",
                "operating_income",
                "ebit",
                "ebitda",
                "net_income",
                "interest_expense",
                "rd_expense",
                "sga_expense",
                "depreciation_amortization",
            ],
        ).sort_values("period")
        bs_df = _frame_from_statements(
            bs_stmts,
            keys=[
                "total_assets",
                "total_equity",
                "total_liabilities",
                "cash_and_equivalents",
                "short_term_debt",
                "long_term_debt",
                "inventory",
                "accounts_receivable",
                "accounts_payable",
                "current_assets",
                "current_liabilities",
                "shares_outstanding",
                "price",
                "market_cap",
                "dividend_per_share",
            ],
        ).sort_values("period")
        cf_df = _frame_from_statements(
            cf_stmts,
            keys=["operating_cash_flow", "capital_expenditures", "free_cash_flow"],
        ).sort_values("period")

        latest_inc, prev_inc = _latest_and_prev(inc_df)
        latest_bs, prev_bs = _latest_and_prev(bs_df)
        latest_cf, _ = _latest_and_prev(cf_df)

        inc_ttm, inc_ttm_period = _ttm_from_df(
            inc_df, keys=["revenue", "net_income", "ebit", "ebitda", "gross_profit", "operating_income", "cogs", "interest_expense"]
        )
        cf_ttm, _ = _ttm_from_df(cf_df, keys=["operating_cash_flow", "capital_expenditures", "free_cash_flow"])

        # Convenience getters
        def g(d: Dict[str, float], key: str) -> float:
            return float(d.get(key)) if (d is not None and key in d and d[key] is not None) else float("nan")

        def avg(key: str) -> float:
            a = g(latest_bs, key)
            b = g(prev_bs, key)
            if np.isnan(a) and np.isnan(b):
                return float("nan")
            if np.isnan(b):
                return a
            if np.isnan(a):
                return b
            return (a + b) / 2.0

        def avg_from_inc(key: str) -> float:
            a = g(latest_inc, key)
            b = g(prev_inc, key)
            if np.isnan(a) and np.isnan(b):
                return float("nan")
            if np.isnan(b):
                return a
            if np.isnan(a):
                return b
            return (a + b) / 2.0

        def sdiv(a: float, b: float) -> float:
            if b is None:
                return float("nan")
            if np.isnan(a) or np.isnan(b) or b == 0.0:
                return float("nan")
            return float(a) / float(b)

        # Base values (latest period)
        revenue = inc_ttm.get("revenue", float("nan")) if inc_ttm else g(latest_inc, "revenue")
        cogs = inc_ttm.get("cogs", float("nan")) if inc_ttm else g(latest_inc, "cogs")
        gross_profit = inc_ttm.get("gross_profit", float("nan")) if inc_ttm else g(latest_inc, "gross_profit")
        operating_income = inc_ttm.get("operating_income", float("nan")) if inc_ttm else g(latest_inc, "operating_income")
        ebit = inc_ttm.get("ebit", float("nan")) if inc_ttm else g(latest_inc, "ebit")
        ebitda = inc_ttm.get("ebitda", float("nan")) if inc_ttm else g(latest_inc, "ebitda")
        net_income = inc_ttm.get("net_income", float("nan")) if inc_ttm else g(latest_inc, "net_income")
        interest_expense = abs(inc_ttm.get("interest_expense", float("nan")) if inc_ttm else g(latest_inc, "interest_expense"))
        rd_expense = g(latest_inc, "rd_expense")
        sga_expense = g(latest_inc, "sga_expense")
        ocf = cf_ttm.get("operating_cash_flow", float("nan")) if cf_ttm else g(latest_cf, "operating_cash_flow")
        capex = cf_ttm.get("capital_expenditures", float("nan")) if cf_ttm else g(latest_cf, "capital_expenditures")
        fcf = cf_ttm.get("free_cash_flow", float("nan")) if cf_ttm else g(latest_cf, "free_cash_flow")
        if np.isnan(fcf):
            if not np.isnan(ocf) and not np.isnan(capex):
                fcf = ocf - capex

        total_assets = g(latest_bs, "total_assets")
        total_equity = g(latest_bs, "total_equity")
        current_assets = g(latest_bs, "current_assets")
        current_liabilities = g(latest_bs, "current_liabilities")
        inventory = g(latest_bs, "inventory")
        accounts_receivable = g(latest_bs, "accounts_receivable")
        accounts_payable = g(latest_bs, "accounts_payable")
        cash = g(latest_bs, "cash_and_equivalents")
        st_debt = g(latest_bs, "short_term_debt")
        lt_debt = g(latest_bs, "long_term_debt")
        total_debt = 0.0
        for v in [st_debt, lt_debt]:
            if not np.isnan(v):
                total_debt += v
        if total_debt == 0.0 and (np.isnan(st_debt) and np.isnan(lt_debt)):
            total_debt = float("nan")

        avg_assets = avg("total_assets")
        avg_equity = avg("total_equity")
        avg_inventory = avg("inventory")
        avg_ar = avg("accounts_receivable")
        avg_ap = avg("accounts_payable")

        shares_out = g(latest_bs, "shares_outstanding")
        price = g(latest_bs, "price")
        market_cap = g(latest_bs, "market_cap")
        if np.isnan(market_cap) and not np.isnan(price) and not np.isnan(shares_out):
            market_cap = price * shares_out

        net_debt = float("nan")
        if not np.isnan(total_debt) and not np.isnan(cash):
            net_debt = max(total_debt - cash, 0.0)

        # Ratios (25+)
        ratios: Dict[str, float] = {
            # Profitability margins
            "gross_margin": sdiv(gross_profit, revenue),
            "operating_margin": sdiv(operating_income, revenue),
            "net_margin": sdiv(net_income, revenue),
            "ebitda_margin": sdiv(ebitda, revenue),
            # Returns
            "roe": sdiv(net_income, avg_equity),
            "roa": sdiv(net_income, avg_assets),
            # Liquidity
            "current_ratio": sdiv(current_assets, current_liabilities),
            "quick_ratio": sdiv(current_assets - (inventory if not np.isnan(inventory) else 0.0), current_liabilities),
            # Leverage
            "debt_to_equity": sdiv(total_debt, total_equity),
            "debt_to_assets": sdiv(total_debt, total_assets),
            "net_debt_to_ebitda": sdiv(net_debt, ebitda),
            # Coverage
            "interest_coverage": sdiv(ebit, interest_expense),
            # Efficiency
            "asset_turnover": sdiv(revenue, avg_assets),
            "inventory_turnover": sdiv(cogs, avg_inventory),
            "receivables_turnover": sdiv(revenue, avg_ar),
            "payables_turnover": sdiv(cogs, avg_ap),
            # Working capital cycle
            "dso": sdiv(accounts_receivable, revenue) * 365.0 if not np.isnan(accounts_receivable) else float("nan"),
            "dio": sdiv(inventory, cogs) * 365.0 if not np.isnan(inventory) else float("nan"),
            "dpo": sdiv(accounts_payable, cogs) * 365.0 if not np.isnan(accounts_payable) else float("nan"),
            # Cash flow
            "fcf_margin": sdiv(fcf, revenue),
            "capex_to_sales": sdiv(capex, revenue),
            "ocf_ratio": sdiv(ocf, current_liabilities),
            # Valuation (requires price/shares, may be NaN)
            "eps": sdiv(net_income, shares_out),
            "pe": sdiv(price, sdiv(net_income, shares_out)),
            "pb": sdiv(price, sdiv(total_equity, shares_out)),
            "pcf": sdiv(price, sdiv(ocf, shares_out)),
            "dividend_yield": sdiv(g(latest_bs, "dividend_per_share"), price),
            # Enterprise value based
            "ev_over_ebitda": sdiv(market_cap + (net_debt if not np.isnan(net_debt) else 0.0), ebitda)
            if not np.isnan(market_cap)
            else float("nan"),
            "ev_over_sales": sdiv(market_cap + (net_debt if not np.isnan(net_debt) else 0.0), revenue)
            if not np.isnan(market_cap)
            else float("nan"),
        }

        # Derived metric: cash conversion cycle
        dio = ratios.get("dio", float("nan"))
        dso = ratios.get("dso", float("nan"))
        dpo = ratios.get("dpo", float("nan"))
        ratios["cash_conversion_cycle"] = (
            dso + dio - dpo if not (np.isnan(dso) or np.isnan(dio) or np.isnan(dpo)) else float("nan")
        )

        metadata = {
            "notes": "Ratios computed using TTM income/CF and latest balance sheet with simple averages for denominators.",
            "latest_period": str(inc_ttm_period if inc_ttm_period is not None else (latest_inc.get("period") if latest_inc else "")),
        }
        return RatioSummary(ratios=ratios, metadata=metadata)


class ValuationEngine:
    """Aggregate multiple valuation methodologies (DCF, PE band, EV/EBITDA, PB, EV/Sales)."""

    def run(
        self,
        dataset: FinancialDataset,
        ratios: RatioSummary,
        overrides: Optional[Dict[str, float]] = None,
        hints: Optional[Dict[str, float]] = None,
    ) -> ValuationBundle:
        # Extract latest facts needed for valuation
        inc_stmts = _dedup_statements(dataset.income_statements)
        bs_stmts = _dedup_statements(dataset.balance_sheets)
        cf_stmts = _dedup_statements(dataset.cash_flows)
        overrides = overrides or {}
        hints = hints or {}

        inc_df = _frame_from_statements(inc_stmts, keys=["net_income", "ebitda", "revenue"]).sort_values("period")
        bs_df = _frame_from_statements(
            bs_stmts,
            keys=[
                "cash_and_equivalents",
                "short_term_debt",
                "long_term_debt",
                "total_equity",
                "shares_outstanding",
                "price",
                "market_cap",
                "wacc",
                "g",
                "terminal_growth",
                "forecast_years",
            ],
        ).sort_values("period")
        cf_df = _frame_from_statements(
            cf_stmts, keys=["free_cash_flow", "operating_cash_flow", "capital_expenditures"]
        ).sort_values("period")

        latest_inc, _ = _latest_and_prev(inc_df)
        latest_bs, _ = _latest_and_prev(bs_df)
        latest_cf, _ = _latest_and_prev(cf_df)

        inc_ttm, _ = _ttm_from_df(inc_df, keys=["net_income", "ebitda", "revenue"])
        cf_ttm, _ = _ttm_from_df(cf_df, keys=["free_cash_flow", "operating_cash_flow", "capital_expenditures"])
        net_margin_ratio = float("nan")
        if ratios and ratios.ratios:
            try:
                net_margin_ratio = float(ratios.ratios.get("net_margin", float("nan")))
            except Exception:
                net_margin_ratio = float("nan")
        warnings: List[str] = []

        def g(d: Dict[str, float], key: str) -> float:
            return float(d.get(key)) if (d is not None and key in d and d[key] is not None) else float("nan")

        def from_override(key: str) -> float:
            """Prefer CLI/config overrides when provided."""
            try:
                value = overrides.get(key)
            except Exception:
                return float("nan")
            return _to_float(value)

        # Inputs
        price = g(latest_bs, "price")
        shares = g(latest_bs, "shares_outstanding")
        market_cap = g(latest_bs, "market_cap")
        if np.isnan(market_cap) and not np.isnan(price) and not np.isnan(shares):
            market_cap = price * shares
        if np.isnan(shares) or shares <= 0:
            warnings.append("shares_outstanding 缺失或无效，部分估值方法可能不可靠。")

        ebitda = inc_ttm.get("ebitda", float("nan")) if inc_ttm else g(latest_inc, "ebitda")
        revenue = inc_ttm.get("revenue", float("nan")) if inc_ttm else g(latest_inc, "revenue")
        net_income = inc_ttm.get("net_income", float("nan")) if inc_ttm else g(latest_inc, "net_income")
        eps = float("nan")
        if not np.isnan(net_income) and not np.isnan(shares) and shares != 0.0:
            eps = net_income / shares

        st_debt = g(latest_bs, "short_term_debt")
        lt_debt = g(latest_bs, "long_term_debt")
        cash = g(latest_bs, "cash_and_equivalents")
        total_debt = 0.0
        for v in [st_debt, lt_debt]:
            if not np.isnan(v):
                total_debt += v
        if total_debt == 0.0 and (np.isnan(st_debt) and np.isnan(lt_debt)):
            total_debt = float("nan")
        net_debt = float("nan")
        if not np.isnan(total_debt) and not np.isnan(cash):
            net_debt = max(total_debt - cash, 0.0)

        fcf = cf_ttm.get("free_cash_flow", float("nan")) if cf_ttm else g(latest_cf, "free_cash_flow")
        if np.isnan(fcf):
            ocf = cf_ttm.get("operating_cash_flow", float("nan")) if cf_ttm else g(latest_cf, "operating_cash_flow")
            capex = cf_ttm.get("capital_expenditures", float("nan")) if cf_ttm else g(latest_cf, "capital_expenditures")
            if not np.isnan(ocf) and not np.isnan(capex):
                fcf = ocf - capex

        # Assumptions with dataset overrides if provided
        derived_defaults = self._derive_assumptions(inc_df, bs_df, ratios)

        def pick_assumption(key: str, dataset_value: float, default_value: float) -> float:
            override_value = from_override(key)
            if not np.isnan(override_value):
                return override_value
            hint_value = _to_float(hints.get(key)) if hints else float("nan")
            if not np.isnan(hint_value):
                return hint_value
            if not np.isnan(dataset_value):
                return dataset_value
            return default_value

        wacc = pick_assumption("wacc", g(latest_bs, "wacc"), derived_defaults.get("wacc", 0.10))
        growth = pick_assumption("g", g(latest_bs, "g"), derived_defaults.get("g", 0.08))
        terminal_growth = pick_assumption(
            "terminal_growth", g(latest_bs, "terminal_growth"), derived_defaults.get("terminal_growth", 0.03)
        )
        years = pick_assumption(
            "forecast_years", g(latest_bs, "forecast_years"), derived_defaults.get("forecast_years", 5.0)
        )
        n_years = max(int(round(years)), 1)

        valuation_methods: Dict[str, Dict[str, float]] = {}

        # DCF (FCFF) -> Equity per share
        dcf_fair_value = float("nan")
        if not (np.isnan(fcf) or np.isnan(net_debt) or np.isnan(shares) or shares == 0.0):
            dcf_equity_value, dcf_per_share = _dcf_fcff(
                fcf=fcf,
                growth=growth,
                wacc=wacc,
                terminal_growth=terminal_growth,
                years=n_years,
                net_debt=net_debt,
                shares=shares,
            )
            dcf_fair_value = dcf_per_share
            method = {
                "fair_value": dcf_per_share,
                "equity_value": dcf_equity_value,
                "wacc": wacc,
                "g": growth,
                "gt": terminal_growth,
                "years": float(n_years),
            }
            # Sensitivity: wacc +/- 2pp, growth +/- 1pp
            for wacc_s in [wacc - 0.02, wacc + 0.02]:
                if wacc_s > terminal_growth and wacc_s > 0:
                    _, per_share = _dcf_fcff(
                        fcf=fcf,
                        growth=growth,
                        wacc=wacc_s,
                        terminal_growth=terminal_growth,
                        years=n_years,
                        net_debt=net_debt,
                        shares=shares,
                    )
                    method[f"fair_value_wacc_{int(round(wacc_s*100))}"] = per_share
            for g_s in [growth - 0.01, growth + 0.01]:
                if g_s < wacc and g_s > -0.5:
                    _, per_share = _dcf_fcff(
                        fcf=fcf,
                        growth=g_s,
                        wacc=wacc,
                        terminal_growth=terminal_growth,
                        years=n_years,
                        net_debt=net_debt,
                        shares=shares,
                    )
                    method[f"fair_value_g_{int(round(g_s*100))}"] = per_share
            if not np.isnan(price):
                method["upside"] = (dcf_per_share / price) - 1.0 if price > 0 else float("nan")
            valuation_methods["dcf"] = method

        # PE band valuation (requires positive EPS/price)
        pe_low = pick_assumption("pe_low", float("nan"), derived_defaults.get("pe_low", 10.0))
        pe_high = pick_assumption("pe_high", float("nan"), derived_defaults.get("pe_high", 20.0))
        if pe_low <= 0 or pe_high <= 0 or pe_low >= pe_high:
            pe_low, pe_high = 10.0, 20.0
        if np.isnan(eps) or eps <= 0:
            warnings.append("EPS/净利润为负或缺失，PE 估值仅作参考。")
        if not (np.isnan(eps) or np.isnan(shares) or np.isnan(price)) and eps > 0 and price > 0:
            pe_mid = (pe_low + pe_high) / 2.0
            fair_value_pe = eps * pe_mid
            method = {
                "fair_value": fair_value_pe,
                "pe_low": pe_low,
                "pe_high": pe_high,
                "eps": eps,
            }
            method["upside"] = (fair_value_pe / price) - 1.0 if price > 0 else float("nan")
            valuation_methods["pe_band"] = method

        # EV/EBITDA valuation
        ev_ebitda_low = pick_assumption("ev_ebitda_low", float("nan"), derived_defaults.get("ev_ebitda_low", 6.0))
        ev_ebitda_high = pick_assumption(
            "ev_ebitda_high", float("nan"), derived_defaults.get("ev_ebitda_high", 12.0)
        )
        if ev_ebitda_low <= 0 or ev_ebitda_high <= 0 or ev_ebitda_low >= ev_ebitda_high:
            ev_ebitda_low, ev_ebitda_high = 6.0, 12.0
        if not (np.isnan(ebitda) or ebitda <= 0.0 or np.isnan(net_debt) or np.isnan(shares)):
            ev_ebitda_mid = (ev_ebitda_low + ev_ebitda_high) / 2.0
            implied_ev = ebitda * ev_ebitda_mid
            implied_equity = implied_ev - net_debt
            fair_value_ev = implied_equity / shares if shares > 0 else float("nan")
            method = {
                "fair_value": fair_value_ev,
                "ev_ebitda_low": ev_ebitda_low,
                "ev_ebitda_high": ev_ebitda_high,
                "ebitda": ebitda,
            }
            if not np.isnan(price):
                method["upside"] = (fair_value_ev / price) - 1.0 if price > 0 else float("nan")
            valuation_methods["ev_ebitda"] = method

        # PB band valuation (book value multiples)
        pb_defaults = (
            derived_defaults.get("pb_low", 0.8),
            derived_defaults.get("pb_high", 1.5),
        )
        pb_low = pick_assumption("pb_low", float("nan"), pb_defaults[0])
        pb_high = pick_assumption("pb_high", float("nan"), pb_defaults[1])
        if pb_low <= 0 or pb_high <= 0 or pb_low >= pb_high:
            pb_low, pb_high = (pb_defaults[0], pb_defaults[1])
        book_per_share = float("nan")
        total_equity = g(latest_bs, "total_equity")
        if not (np.isnan(total_equity) or np.isnan(shares) or shares == 0.0):
            book_per_share = total_equity / shares
        if not np.isnan(book_per_share) and book_per_share > 0:
            pb_mid = (pb_low + pb_high) / 2.0
            fair_value_pb = book_per_share * pb_mid
            method = {
                "fair_value": fair_value_pb,
                "pb_low": pb_low,
                "pb_high": pb_high,
                "book_per_share": book_per_share,
            }
            if not np.isnan(price) and price > 0:
                method["upside"] = (fair_value_pb / price) - 1.0
            valuation_methods["pb_band"] = method

        # EV/Sales valuation
        sales_defaults = (
            derived_defaults.get("ev_sales_low", 1.0),
            derived_defaults.get("ev_sales_high", 2.0),
        )
        ev_sales_low = pick_assumption("ev_sales_low", float("nan"), sales_defaults[0])
        ev_sales_high = pick_assumption("ev_sales_high", float("nan"), sales_defaults[1])
        if ev_sales_low <= 0 or ev_sales_high <= 0 or ev_sales_low >= ev_sales_high:
            ev_sales_low, ev_sales_high = sales_defaults
        if not (np.isnan(revenue) or revenue <= 0.0 or np.isnan(net_debt) or np.isnan(shares) or shares == 0.0):
            ev_sales_mid = (ev_sales_low + ev_sales_high) / 2.0
            implied_ev = revenue * ev_sales_mid
            implied_equity = implied_ev - net_debt
            fair_value_sales = implied_equity / shares if shares > 0 else float("nan")
            method = {
                "fair_value": fair_value_sales,
                "ev_sales_low": ev_sales_low,
                "ev_sales_high": ev_sales_high,
                "ttm_revenue": revenue,
            }
            if not np.isnan(price) and price > 0:
                method["upside"] = (fair_value_sales / price) - 1.0
            valuation_methods["ev_sales"] = method

        # Scenario set (base/bull/bear) using DCF backbone with ±1pp WACC / ±1pp g
        scenarios: List[Dict[str, float]] = []
        # Choose an intrinsic value (simple average of available methods)
        fair_values = [m.get("fair_value") for m in valuation_methods.values() if not np.isnan(m.get("fair_value", float("nan")))]
        intrinsic = float(np.nanmean(fair_values)) if fair_values else None
        base_val = intrinsic if intrinsic is not None else (dcf_fair_value if not np.isnan(dcf_fair_value) else float("nan"))
        if not np.isnan(base_val):
            def _scenario(label: str, w: float, g_val: float) -> Dict[str, float]:
                fv = base_val
                if not np.isnan(dcf_fair_value):
                    _, per_share = _dcf_fcff(
                        fcf=fcf if not np.isnan(fcf) else 0.0,
                        growth=g_val,
                        wacc=w,
                        terminal_growth=terminal_growth,
                        years=n_years,
                        net_debt=net_debt if not np.isnan(net_debt) else 0.0,
                        shares=shares if shares > 0 else 1.0,
                    )
                    fv = per_share
                return {"case": label, "fair_value": float(fv), "wacc": float(w), "g": float(g_val)}

            bull_wacc = max(wacc - 0.01, 0.05)
            bull_g = min(growth + 0.01, bull_wacc - 0.005) if bull_wacc > 0.01 else growth
            bear_wacc = min(wacc + 0.01, 0.20)
            bear_g = max(growth - 0.01, -0.05)
            scenarios = [
                _scenario("base", wacc, growth),
                _scenario("bull", bull_wacc, bull_g),
                _scenario("bear", bear_wacc, bear_g),
            ]
            valuation_methods["scenarios"] = {"cases": scenarios}

        merged_assumptions = dict(derived_defaults)
        for k, v in (hints or {}).items():
            if not np.isnan(_to_float(v)):
                merged_assumptions[k] = _to_float(v)
        return ValuationBundle(intrinsic_value=intrinsic, valuation_methods=valuation_methods, assumptions=merged_assumptions, warnings=warnings)

    def _derive_assumptions(self, inc_df: pd.DataFrame, bs_df: pd.DataFrame, ratios: RatioSummary) -> Dict[str, float]:
        """Derive ticker-specific valuation priors from financials and ratios."""
        # Profitability & leverage signals
        revenue_cagr = _cagr_from_df(_filter_annual(inc_df), "revenue")
        rev_vol = float("nan")
        try:
            rev_vol = float(inc_df.sort_values("period")["revenue"].pct_change().std())
        except Exception:
            rev_vol = float("nan")
        net_margin = float("nan")
        debt_to_equity = float("nan")
        if ratios and ratios.ratios:
            net_margin = _to_float(ratios.ratios.get("net_margin"))
            debt_to_equity = _to_float(ratios.ratios.get("debt_to_equity"))
        if np.isnan(debt_to_equity):
            try:
                latest_bs, _ = _latest_and_prev(bs_df)
                total_liab = _to_float(latest_bs.get("total_liabilities")) if latest_bs else float("nan")
                total_equity = _to_float(latest_bs.get("total_equity")) if latest_bs else float("nan")
                if not np.isnan(total_liab) and not np.isnan(total_equity) and total_equity != 0:
                    debt_to_equity = total_liab / total_equity
            except Exception:
                debt_to_equity = float("nan")

        # WACC heuristic: start at 9%, adjust by leverage/profitability
        wacc = 0.09
        if not np.isnan(debt_to_equity):
            if debt_to_equity > 1.5:
                wacc += 0.015
            elif debt_to_equity > 0.8:
                wacc += 0.008
            elif debt_to_equity < 0.4:
                wacc -= 0.005
        if not np.isnan(net_margin):
            if net_margin < 0:
                wacc += 0.01
            elif net_margin > 0.15:
                wacc -= 0.005
        wacc = float(min(max(wacc, 0.07), 0.15))

        # Growth heuristic: use revenue CAGR, adjust by volatility to avoid over-exuberance
        growth_base = 0.05 if np.isnan(revenue_cagr) else revenue_cagr
        vol_haircut = 0.0 if np.isnan(rev_vol) else min(max(rev_vol, 0.0), 0.1)
        growth = growth_base - vol_haircut * 0.3
        growth = float(min(max(growth, -0.03), 0.12))
        terminal_growth = float(min(max(growth * 0.5, 0.01), 0.03))
        forecast_years = 5.0

        # Multiple bands based on profitability class
        pe_low, pe_high = 10.0, 20.0
        ev_ebitda_low, ev_ebitda_high = 6.0, 12.0
        pb_low, pb_high = 0.8, 1.4
        ev_sales_low, ev_sales_high = 1.0, 2.0
        if not np.isnan(net_margin):
            if net_margin <= 0:
                pe_low, pe_high = 8.0, 14.0
                ev_ebitda_low, ev_ebitda_high = 5.0, 9.0
                pb_low, pb_high = 0.4, 0.9
                ev_sales_low, ev_sales_high = 0.2, 0.8
            elif net_margin < 0.05:
                pe_low, pe_high = 9.0, 17.0
                ev_ebitda_low, ev_ebitda_high = 6.0, 10.0
                pb_low, pb_high = 0.7, 1.2
                ev_sales_low, ev_sales_high = 0.8, 1.6
            elif net_margin < 0.15:
                pe_low, pe_high = 11.0, 19.0
                ev_ebitda_low, ev_ebitda_high = 7.0, 11.0
                pb_low, pb_high = 0.8, 1.4
                ev_sales_low, ev_sales_high = 1.0, 2.0
            else:
                pe_low, pe_high = 12.0, 22.0
                ev_ebitda_low, ev_ebitda_high = 7.0, 12.0
                pb_low, pb_high = 1.2, 2.2
                ev_sales_low, ev_sales_high = 1.5, 3.0

        return {
            "wacc": wacc,
            "g": growth,
            "terminal_growth": terminal_growth,
            "forecast_years": forecast_years,
            "pe_low": pe_low,
            "pe_high": pe_high,
            "ev_ebitda_low": ev_ebitda_low,
            "ev_ebitda_high": ev_ebitda_high,
            "pb_low": pb_low,
            "pb_high": pb_high,
            "ev_sales_low": ev_sales_low,
            "ev_sales_high": ev_sales_high,
        }


# ----------------------------
# Internal helpers
# ----------------------------

def _to_float(value: float) -> float:
    try:
        if value is None:
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _flag_value(flag: Optional[int]) -> int:
    try:
        return int(flag) if flag is not None else 0
    except Exception:
        return 0


def _dedup_statements(statements: Iterable) -> List:
    """Deduplicate statements by period, preferring revised update_flag and latest announced_date."""
    best: Dict[str, object] = {}
    for s in statements:
        key = str(getattr(s, "period", None))
        flag = _flag_value(getattr(s, "update_flag", None))
        if key not in best:
            best[key] = s
            continue
        prev = best[key]
        prev_flag = _flag_value(getattr(prev, "update_flag", None))
        prev_ann = getattr(prev, "announced_date", datetime.min.date())
        ann = getattr(s, "announced_date", datetime.min.date())
        if flag > prev_flag or (flag == prev_flag and ann >= prev_ann):
            best[key] = s
    return sorted(best.values(), key=lambda x: getattr(x, "period", None))


def _filter_annual(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "period" not in df:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    return df[df["period"].dt.month == 12] if hasattr(df["period"], "dt") else df


def _yoy(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df:
        return float("nan")
    df = df.sort_values("period")
    latest = df.iloc[-1]
    latest_date = latest.get("period")
    if pd.isna(latest_date):
        return float("nan")
    try:
        latest_date = pd.to_datetime(latest_date)
    except Exception:
        return float("nan")
    # Annual YoY
    if latest_date.month == 12:
        annual = _filter_annual(df)
        if len(annual) < 2:
            return float("nan")
        annual = annual.sort_values("period")
        return _to_float(annual[col].pct_change().iloc[-1])
    # Same quarter last year
    mask = (df["period"].dt.month == latest_date.month) & (df["period"].dt.year == latest_date.year - 1)
    prev_candidates = df[mask]
    if prev_candidates.empty:
        return float("nan")
    prev_val = prev_candidates.iloc[-1][col]
    if pd.isna(prev_val) or pd.isna(latest[col]) or prev_val == 0:
        return float("nan")
    try:
        return (float(latest[col]) - float(prev_val)) / float(prev_val)
    except Exception:
        return float("nan")


def _ttm_from_df(df: pd.DataFrame, keys: List[str]) -> Tuple[Dict[str, float], Optional[object]]:
    """Compute trailing-twelve-month sums from latest four periods (best-effort)."""
    if df is None or df.empty:
        return {}, None
    df = df.sort_values("period", ascending=False).reset_index(drop=True)
    window = df.head(4)
    months = set()
    try:
        months = set(window["period"].dt.month.dropna().astype(int).tolist())
    except Exception:
        months = set()
    use_single_period = months == {12} or len(window) == 1
    totals: Dict[str, float] = {}
    for key in keys:
        series = window.get(key, [])
        if use_single_period:
            value = series.iloc[0] if len(series) else float("nan")
            totals[key] = float(value) if not pd.isna(value) else float("nan")
        else:
            values = [v for v in series if not pd.isna(v)]
            totals[key] = float(np.nansum(values)) if values else float("nan")
    end_period = window.iloc[0].get("period")
    return totals, end_period


def _frame_from_statements(statements: Iterable, keys: List[str]) -> pd.DataFrame:
    rows: List[Dict[str, float]] = []
    for s in statements:
        row: Dict[str, float] = {k: _to_float(s.metrics.get(k)) for k in keys if s.metrics is not None}
        row["period"] = getattr(s, "period", None)
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=["period", *keys])
    df = pd.DataFrame(rows)
    # Ensure period is datetime-like for sorting; if not available, keep as is
    if "period" in df.columns:
        try:
            df["period"] = pd.to_datetime(df["period"])  # type: ignore[arg-type]
        except Exception:
            pass
    return df


def _latest_and_prev(df: pd.DataFrame) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, float]]]:
    if df is None or df.empty:
        return None, None
    df = df.sort_values("period").reset_index(drop=True)
    latest = df.iloc[-1].to_dict()
    prev = df.iloc[-2].to_dict() if len(df) > 1 else None
    return latest, prev


def _cagr_from_df(df: pd.DataFrame, col: str) -> float:
    if col not in df or df[col].dropna().empty or len(df) < 2:
        return float("nan")
    series = df[["period", col]].dropna()
    series = series.sort_values("period")
    start_val = float(series[col].iloc[0])
    end_val = float(series[col].iloc[-1])
    if start_val <= 0 or end_val <= 0:
        return float("nan")
    # Assume annual data; periods is count-1 if date math isn't reliable
    try:
        start_date = pd.to_datetime(series["period"].iloc[0]).date()
        end_date = pd.to_datetime(series["period"].iloc[-1]).date()
        years = max((end_date.year - start_date.year), 1)
    except Exception:
        years = max(len(series) - 1, 1)
    return (end_val / start_val) ** (1.0 / years) - 1.0


def _dcf_fcff(
    *,
    fcf: float,
    growth: float,
    wacc: float,
    terminal_growth: float,
    years: int,
    net_debt: float,
    shares: float,
) -> Tuple[float, float]:
    """Compute FCFF-based DCF returning (equity_value, per_share).

    - Forecast FCF with constant growth for N years.
    - Discount at WACC, compute terminal value using Gordon Growth.
    - Enterprise value is PV of forecast + PV of terminal; subtract net debt for equity value.
    """
    if years <= 0 or wacc <= terminal_growth:
        return float("nan"), float("nan")
    cash_flows = [fcf * ((1.0 + growth) ** t) for t in range(1, years + 1)]
    discounts = [(1.0 + wacc) ** t for t in range(1, years + 1)]
    pv_flows = sum(cf / d for cf, d in zip(cash_flows, discounts))
    terminal_cf = cash_flows[-1] * (1.0 + terminal_growth)
    terminal_value = terminal_cf / (wacc - terminal_growth)
    pv_terminal = terminal_value / ((1.0 + wacc) ** years)
    enterprise_value = pv_flows + pv_terminal
    equity_value = enterprise_value - (net_debt if not np.isnan(net_debt) else 0.0)
    per_share = equity_value / shares if shares > 0 else float("nan")
    return float(equity_value), float(per_share)
