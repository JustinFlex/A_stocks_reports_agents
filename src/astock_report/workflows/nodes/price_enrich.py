"""LangGraph node to enrich state with recent price/volume context."""
from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional

import pandas as pd
import numpy as np
import math

from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState

DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_INDEX_CODE = "000300.SH"  # CSI300 as broad market proxy


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
    # Compute beta/WACC hint vs market index if possible
    _attach_market_hints(state, context, sorted_frame)
    return state


def _select_index_code(industry: Optional[str]) -> str:
    return DEFAULT_INDEX_CODE  # Placeholder; now resolved via sector_service when available


def _compute_beta(ticker_df: pd.DataFrame, index_df: pd.DataFrame) -> float:
    try:
        t = ticker_df[["trade_date", "close"]].copy()
        i = index_df[["trade_date", "close"]].copy()
        t["trade_date"] = pd.to_datetime(t["trade_date"])
        i["trade_date"] = pd.to_datetime(i["trade_date"])
        t = t.sort_values("trade_date")
        i = i.sort_values("trade_date")
        t["ret"] = t["close"].pct_change()
        i["ret"] = i["close"].pct_change()
        merged = pd.merge(t[["trade_date", "ret"]], i[["trade_date", "ret"]], on="trade_date", suffixes=("_t", "_i"))
        merged = merged.dropna()
        if merged.empty or merged["ret_i"].var() == 0:
            return float("nan")
        cov = np.cov(merged["ret_t"], merged["ret_i"])[0, 1]
        var = merged["ret_i"].var()
        return float(cov / var)
    except Exception:
        return float("nan")


def _attach_market_hints(state: ReportState, context: WorkflowContext, price_df: pd.DataFrame) -> None:
    ticker = state.get("ticker")
    if price_df is None or price_df.empty or context.tushare is None:
        return
    basic = state.get("basic_info") or {}
    sector_info = None
    if context.sector_service:
        sector_info = context.sector_service.resolve_sw_index(ticker, basic.get("industry"))
    index_code = sector_info.get("index_code") if sector_info else None
    if not index_code:
        index_code = _select_index_code(str(basic.get("industry")))

    end_date = date.today()
    start_date = end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    try:
        # Cache index prices in same table keyed by index_code
        idx_history = context.repository.fetch_prices(index_code, start_date=start_date, end_date=end_date, limit=DEFAULT_LOOKBACK_DAYS)
        if not idx_history:
            if index_code.endswith(".SI"):
                idx_frame = context.tushare.fetch_sw_daily(
                    index_code,
                    start_date=start_date,
                    end_date=end_date,
                    limit=DEFAULT_LOOKBACK_DAYS,
                )
            else:
                idx_frame = context.tushare.fetch_index_daily(
                    index_code,
                    start_date=start_date,
                    end_date=end_date,
                    limit=DEFAULT_LOOKBACK_DAYS,
                )
            idx_history = []
            if idx_frame is not None and not idx_frame.empty:
                idx_sorted = idx_frame.sort_values("trade_date", ascending=False)
                idx_history = idx_sorted.to_dict(orient="records")
                context.repository.upsert_prices(index_code, idx_history)
        if not idx_history:
            return
        idx_df = pd.DataFrame(idx_history)
        beta = _compute_beta(price_df, idx_df)
        if np.isnan(beta):
            return
        # Build WACC hint using CAPM
        rf = 0.03
        erp = 0.06
        ke = rf + beta * erp
        # Debt/Equity for weights
        de_ratio = float("nan")
        fin = state.get("financials")
        if fin and fin.balance_sheets:
            latest_bs = fin.balance_sheets[-1]
            total_equity = latest_bs.metrics.get("total_equity")
            total_debt = (latest_bs.metrics.get("short_term_debt") or 0.0) + (latest_bs.metrics.get("long_term_debt") or 0.0)
            try:
                if total_equity:
                    de_ratio = float(total_debt) / float(total_equity)
            except Exception:
                de_ratio = float("nan")
        tax = 0.25
        kd = 0.04
        if np.isnan(de_ratio):
            wacc = ke
        else:
            e_weight = 1.0 / (1.0 + de_ratio)
            d_weight = 1.0 - e_weight
            wacc = ke * e_weight + kd * (1 - tax) * d_weight
        hints = state.setdefault("valuation_hints", {})
        hints.update(
            {
                "beta": beta,
                "wacc": float(min(max(wacc, 0.06), 0.18)),
                "index_code": index_code,
                "index_name": sector_info.get("index_name") if sector_info else None,
                "sector_level": sector_info.get("level") if sector_info else None,
                "sector_member_count": sector_info.get("member_count") if sector_info else None,
            }
        )

        # Sector multiple anchors from peer percentiles when Shenwan index is available
        latest_trade_date = price_df.iloc[0].get("trade_date")
        if context.sector_service and index_code.endswith(".SI"):
            peer = context.sector_service.peer_percentiles(index_code, trade_date=latest_trade_date)
            if peer:
                hints["peer_percentiles"] = peer
                pe = peer.get("pe") or {}
                pb = peer.get("pb") or {}
                ps = peer.get("ps") or {}

                def _apply_band(low_key: str, high_key: str, bucket: dict) -> None:
                    low = bucket.get("p25") or bucket.get("p20")
                    high = bucket.get("p75") or bucket.get("p80")
                    if low and high and not (math.isnan(low) or math.isnan(high)) and low > 0 and high > low:
                        hints[low_key] = float(low)
                        hints[high_key] = float(high)

                _apply_band("pe_low", "pe_high", pe)
                _apply_band("pb_low", "pb_high", pb)
                _apply_band("ev_sales_low", "ev_sales_high", ps)
        # Broad fallback using index_dailybasic percentiles if peer data missing
        wants_pe = "pe_low" not in hints or "pe_high" not in hints
        wants_pb = "pb_low" not in hints or "pb_high" not in hints
        wants_ps = "ev_sales_low" not in hints or "ev_sales_high" not in hints
        if wants_pe or wants_pb or wants_ps:
            try:
                basic_frame = context.tushare.fetch_index_dailybasic(
                    index_code,
                    start_date=start_date,
                    end_date=end_date,
                    limit=DEFAULT_LOOKBACK_DAYS,
                )
                if basic_frame is not None and not basic_frame.empty:
                    df = basic_frame.sort_values("trade_date")

                    def pct(series, p):
                        vals = series.dropna().astype(float)
                        return float(np.percentile(vals, p)) if len(vals) else float("nan")

                    pe_low = pct(df["pe_ttm"], 20)
                    pe_high = pct(df["pe_ttm"], 80)
                    pb_low = pct(df["pb"], 20)
                    pb_high = pct(df["pb"], 80)
                    ps_low = pct(df.get("ps_ttm", pd.Series([])), 20) if "ps_ttm" in df else float("nan")
                    ps_high = pct(df.get("ps_ttm", pd.Series([])), 80) if "ps_ttm" in df else float("nan")
                    if wants_pe and not math.isnan(pe_low) and not math.isnan(pe_high) and pe_low > 0 and pe_high > pe_low:
                        hints["pe_low"] = pe_low
                        hints["pe_high"] = pe_high
                    if wants_pb and not math.isnan(pb_low) and not math.isnan(pb_high) and pb_low > 0 and pb_high > pb_low:
                        hints["pb_low"] = pb_low
                        hints["pb_high"] = pb_high
                    if wants_ps and not math.isnan(ps_low) and not math.isnan(ps_high) and ps_low > 0 and ps_high > ps_low:
                        hints["ev_sales_low"] = ps_low
                        hints["ev_sales_high"] = ps_high
            except Exception:
                pass
    except Exception as exc:  # pylint: disable=broad-except
        errors = state.setdefault("errors", [])
        errors.append(f"PriceEnrichAgent -> beta/WACC hint failed for {ticker}: {exc}")
    except Exception as exc:  # pylint: disable=broad-except
        errors = state.setdefault("errors", [])
        errors.append(f"PriceEnrichAgent -> beta/WACC hint failed for {ticker}: {exc}")
