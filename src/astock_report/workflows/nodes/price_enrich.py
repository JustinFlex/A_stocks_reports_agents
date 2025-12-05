"""LangGraph node to enrich state with recent price/volume context."""
from __future__ import annotations

from datetime import date, timedelta
from typing import List

import pandas as pd

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

DEFAULT_LOOKBACK_DAYS = 120


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    ticker = state["ticker"]

    logs.append("PriceEnrichAgent -> fetch recent price/volume window")

    end_date = date.today()
    start_date = end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    history: List[dict] = []

    # Cache-first lookup
    cached = context.repository.fetch_prices(ticker, start_date=start_date, end_date=end_date, limit=DEFAULT_LOOKBACK_DAYS)
    if cached:
        history = cached
        logs.append(f"PriceEnrichAgent -> loaded {len(history)} cached price rows")
    elif context.tushare is not None:
        try:
            frame = context.tushare.fetch_prices(
                ticker,
                start_date=start_date,
                end_date=end_date,
                limit=DEFAULT_LOOKBACK_DAYS,
            )
            if frame is not None and not frame.empty:
                frame_sorted = frame.sort_values("trade_date", ascending=False)
                history = frame_sorted.to_dict(orient="records")
                context.repository.upsert_prices(ticker, history)
                logs.append(f"PriceEnrichAgent -> cached {len(history)} price rows from TuShare")
            else:
                logs.append("PriceEnrichAgent -> TuShare returned no price data")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"Price enrich failed: {exc}")
            return state
    else:
        # Try price anchor fallback for offline valuation
        anchor = context.repository.fetch_price_anchor(ticker)
        if anchor and anchor.get("close") is not None:
            state["current_price"] = float(anchor["close"])
            state.setdefault("extras", {})["price_anchor"] = anchor
            logs.append("PriceEnrichAgent -> used cached price anchor")
        else:
            logs.append("PriceEnrichAgent -> skipped (TuShare not configured)")
        return state

    if not history:
        return state

    sorted_frame = pd.DataFrame(history).sort_values("trade_date", ascending=False)
    state["price_history"] = sorted_frame.to_dict(orient="records")

    extras = state.setdefault("extras", {})
    extras["price_window_days"] = DEFAULT_LOOKBACK_DAYS
    extras["price_points"] = len(history)

    if not sorted_frame.empty and "close" in sorted_frame.columns:
        latest = sorted_frame.iloc[0]
        close_value = latest.get("close")
        if close_value is not None:
            state["current_price"] = float(close_value)
            # Persist anchor (with market cap if shares info available later)
            shares = None
            financials = state.get("financials")
            if financials and financials.balance_sheets:
                latest_bs = financials.balance_sheets[-1]
                shares = latest_bs.metrics.get("shares_outstanding")
            market_cap = None
            try:
                if shares is not None:
                    market_cap = float(close_value) * float(shares)
            except Exception:
                market_cap = None
            context.repository.upsert_price_anchor(
                ticker=ticker,
                trade_date=str(latest.get("trade_date")),
                close=float(close_value) if close_value is not None else None,
                market_cap=market_cap,
            )
        extras["last_trade_date"] = latest.get("trade_date")
        extras["price_stats"] = {
            "min_close": float(sorted_frame["close"].min()),
            "max_close": float(sorted_frame["close"].max()),
            "avg_close": float(sorted_frame["close"].mean()),
        }
    return state
