"""LangGraph node for loading core financial data from storage or TuShare."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List

import pandas as pd

from astock_report.domain.models.financials import FinancialDataset, FinancialStatement
from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    """Populate the workflow state with structured financial data (cache-first)."""
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    ticker = state["ticker"]

    logs.append("DataLoadAgent -> lookup financial statements")

    records = context.repository.fetch_statements(ticker)
    dataset = _rows_to_dataset(ticker, records)

    if dataset.is_complete():
        logs.append("Loaded cached statements from SQLite.")
    elif context.tushare is not None:
        logs.append("No cached data found; fallback to TuShare API.")
        try:
            raw_frames = context.tushare.fetch_financials(ticker)
            state.setdefault("extras", {})["tushare_raw"] = {
                k: v.head(3).to_dict(orient="records") if hasattr(v, "to_dict") else v for k, v in raw_frames.items()
            }
            normalized = _normalize_tushare_financials(raw_frames, ticker)
            persisted = context.repository.upsert_statements(ticker, normalized["rows"])
            now_utc = datetime.now(timezone.utc)
            logs.append(f"Fetched and cached {persisted} statement rows from TuShare at {now_utc.isoformat()}")
            dataset = normalized["dataset"]
            # Basic info & holders (best effort)
            _load_and_cache_basic_info(state, context)
            _load_and_cache_holders(state, context)
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"TuShare fetch failed: {exc}")
    else:
        errors.append("Neither cached data nor TuShare client is available.")

    if not dataset.is_complete():
        # Seed placeholder statements so downstream agents can execute during scaffolding.
        now = datetime.now(timezone.utc).date()
        dataset.income_statements.append(
            FinancialStatement(ticker=ticker, period=now, statement_type="IS", metrics={})
        )
        dataset.balance_sheets.append(
            FinancialStatement(ticker=ticker, period=now, statement_type="BS", metrics={})
        )
        dataset.cash_flows.append(
            FinancialStatement(ticker=ticker, period=now, statement_type="CF", metrics={})
        )

    state["financials"] = dataset
    return state


# -----------------
# Normalization
# -----------------


INCOME_MAP = {
    "revenue": ["revenue", "total_revenue"],
    "cogs": ["oper_cost", "total_cogs"],
    "gross_profit": ["gross_profit", "grossprofit"],
    "operating_income": ["operate_profit", "oper_profit", "total_profit"],
    "ebit": ["ebit", "total_profit"],
    "ebitda": ["ebitda"],
    "net_income": ["n_income", "netprofit", "n_income_attr_p"],
    "interest_expense": ["int_exp", "fin_exp"],
    "rd_expense": ["rd_exp"],
    "sga_expense": ["sell_exp", "admin_exp"],
}

BALANCE_MAP = {
    "total_assets": ["total_assets"],
    "total_equity": ["total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int"],
    "total_liabilities": ["total_liab"],
    "cash_and_equivalents": ["money_cap", "cash_and_equivalents"],
    "short_term_debt": ["short_term_borr", "shortterm_loan", "st_borrow"],
    "long_term_debt": ["long_term_borr", "lt_borrow", "bond_payable", "non_cur_liab_due_1y"],
    "inventory": ["inventories", "inventory"],
    "accounts_receivable": ["accounts_receivable", "acct_rcv"],
    "accounts_payable": ["acct_payable", "accounts_payable", "notes_payable"],
    "current_assets": ["total_cur_assets"],
    "current_liabilities": ["total_cur_liab"],
    "shares_outstanding": ["total_share"],
    "dividend_per_share": ["dividend"],
}

CASHFLOW_MAP = {
    "operating_cash_flow": ["n_cashflow_act", "net_cash_flows_oper_act"],
    "capital_expenditures": ["c_paid_acq_const_fiolta", "c_paid_invest", "c_pur_fa_olta", "capital_expenditures"],
    "free_cash_flow": ["free_cash_flow"],
}


def _normalize_tushare_financials(raw_frames: Dict[str, pd.DataFrame], ticker: str):
    """Convert TuShare DataFrames into normalized rows and dataclasses."""
    dataset = FinancialDataset(ticker=ticker)
    rows: List[Dict[str, object]] = []

    income = raw_frames.get("income")
    balance = raw_frames.get("balance")
    cashflow = raw_frames.get("cashflow")

    if income is not None and not income.empty:
        _append_from_frame(dataset.income_statements, rows, ticker, "IS", income, INCOME_MAP)
    if balance is not None and not balance.empty:
        _append_from_frame(dataset.balance_sheets, rows, ticker, "BS", balance, BALANCE_MAP)
    if cashflow is not None and not cashflow.empty:
        _append_from_frame(dataset.cash_flows, rows, ticker, "CF", cashflow, CASHFLOW_MAP)

    dataset = _dedup_dataset(dataset)
    rows = _filter_rows_by_dataset(dataset, rows)

    return {"dataset": dataset, "rows": rows}


def _append_from_frame(
    collector: List[FinancialStatement],
    rows_out: List[Dict[str, object]],
    ticker: str,
    statement_type: str,
    frame: pd.DataFrame,
    mapping: Dict[str, List[str]],
) -> None:
    for _, r in frame.iterrows():
        period = _safe_date(r.get("end_date") or r.get("report_date") or r.get("f_ann_date"))
        period = period or datetime.now(timezone.utc).date()
        announced = _safe_date(r.get("ann_date"))
        update_flag = _safe_int(r.get("update_flag"))
        frequency = _infer_frequency(period)
        metrics: Dict[str, float] = {}
        for canonical, candidates in mapping.items():
            value = _first_present(r, candidates)
            if value is None:
                continue
            metrics[canonical] = float(value)
            rows_out.append(
                {
                    "ticker": ticker,
                    "report_type": statement_type,
                    "report_date": str(period),
                    "metric": canonical,
                    "value": float(value),
                }
            )
        # Derived FCF when possible
        if statement_type == "CF" and "free_cash_flow" not in metrics:
            ocf = metrics.get("operating_cash_flow")
            capex = metrics.get("capital_expenditures")
            if ocf is not None and capex is not None:
                metrics["free_cash_flow"] = float(ocf) - float(capex)
        collector.append(
            FinancialStatement(
                ticker=ticker,
                period=period,
                statement_type=statement_type,
                metrics=metrics,
                frequency=frequency,
                update_flag=update_flag,
                announced_date=announced,
            )
        )


def _first_present(row: pd.Series, candidates: Iterable[str]):
    for key in candidates:
        if key in row and pd.notna(row[key]):
            return row[key]
    return None


def _safe_date(value):
    try:
        return pd.to_datetime(value).date() if value else None
    except Exception:
        return None


def _rows_to_dataset(ticker: str, records: Dict[str, List[Dict[str, object]]]) -> FinancialDataset:
    dataset = FinancialDataset(ticker=ticker)
    for stype, entries in records.items():
        grouped: Dict[str, Dict[str, float]] = {}
        for row in entries:
            key = str(row.get("report_date"))
            grouped.setdefault(key, {})[str(row.get("metric"))] = _safe_float(row.get("value"))
        for period, metrics in grouped.items():
            stmt = FinancialStatement(
                ticker=ticker,
                period=_safe_date(period) or datetime.now(timezone.utc).date(),
                statement_type=stype,
                metrics=metrics,
            )
            if stype == "IS":
                dataset.income_statements.append(stmt)
            elif stype == "BS":
                dataset.balance_sheets.append(stmt)
            elif stype == "CF":
                dataset.cash_flows.append(stmt)
    return dataset


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _safe_int(value):
    try:
        return int(value) if value is not None and value != "" else None
    except Exception:
        return None


def _infer_frequency(period):
    try:
        month = getattr(period, "month", None)
        if month == 12:
            return "annual"
        if month in (3, 6, 9):
            return "quarterly"
    except Exception:
        return None
    return None


def _dedup_statements(stmts: List[FinancialStatement]) -> List[FinancialStatement]:
    best: Dict[str, FinancialStatement] = {}
    for s in stmts:
        key = str(s.period)
        flag = s.update_flag if s.update_flag is not None else 0
        if key not in best:
            best[key] = s
            continue
        prev_flag = best[key].update_flag if best[key].update_flag is not None else 0
        prev_ann = best[key].announced_date or datetime.min.date()
        ann = s.announced_date or datetime.min.date()
        if flag > prev_flag or (flag == prev_flag and ann >= prev_ann):
            best[key] = s
    return sorted(best.values(), key=lambda x: x.period)


def _dedup_dataset(dataset: FinancialDataset) -> FinancialDataset:
    dataset.income_statements = _dedup_statements(dataset.income_statements)
    dataset.balance_sheets = _dedup_statements(dataset.balance_sheets)
    dataset.cash_flows = _dedup_statements(dataset.cash_flows)
    return dataset


def _filter_rows_by_dataset(dataset: FinancialDataset, rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    keep_keys = set()
    for stmt in dataset.income_statements:
        keep_keys.add(("IS", str(stmt.period)))
    for stmt in dataset.balance_sheets:
        keep_keys.add(("BS", str(stmt.period)))
    for stmt in dataset.cash_flows:
        keep_keys.add(("CF", str(stmt.period)))
    filtered = [r for r in rows if (r.get("report_type"), str(r.get("report_date"))) in keep_keys]
    return filtered


def _load_and_cache_basic_info(state: ReportState, context: WorkflowContext) -> None:
    if context.tushare is None:
        return
    ticker = state.get("ticker")
    try:
        cached = context.repository.fetch_basic_info(ticker)
        if cached:
            state.setdefault("basic_info", cached)
            return
        frame = context.tushare.fetch_basic_info(ticker)
        if frame is not None and not frame.empty:
            record = frame.iloc[0].to_dict()
            context.repository.upsert_basic_info(record)
            state.setdefault("basic_info", record)
    except Exception:
        # Failing to cache basic info should not halt the workflow.
        return


def _load_and_cache_holders(state: ReportState, context: WorkflowContext) -> None:
    if context.tushare is None:
        return
    ticker = state.get("ticker")
    try:
        cached = context.repository.fetch_holders(ticker)
        if cached:
            state.setdefault("holders", cached)
            return
        frame = context.tushare.fetch_top10_holders(ticker)
        if frame is not None and not frame.empty:
            records = frame.to_dict(orient="records")
            context.repository.upsert_holders(ticker, records)
            state.setdefault("holders", records)
    except Exception:
        return
