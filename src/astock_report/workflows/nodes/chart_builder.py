"""Chart builder node to render price and revenue/profit charts into state."""
from __future__ import annotations

import io
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt

from astock_report.domain.models.financials import FinancialDataset
from astock_report.workflows.context import WorkflowContext
from astock_report.workflows.state import ReportState


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _maybe_save_chart(fig, path: Path, logs, errors, caption: str, charts: List[dict]):
    try:
        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        plt.close(fig)
        path.write_bytes(buf.getvalue())
        charts.append({"path": str(path), "caption": caption})
        logs.append(f"ChartBuilder -> saved chart to {path}")
    except Exception as exc:  # pylint: disable=broad-except
        errors.append(f"ChartBuilder save failed: {exc}")


def _clean_income_statements(statements):
    """Return one statement per period, preferring revised data, filtered to annual if available."""
    best = {}
    for s in statements:
        key = str(getattr(s, "period", None))
        flag = getattr(s, "update_flag", None) or 0
        ann = getattr(s, "announced_date", None)
        if key not in best:
            best[key] = s
            continue
        prev = best[key]
        prev_flag = getattr(prev, "update_flag", None) or 0
        prev_ann = getattr(prev, "announced_date", None)
        if flag > prev_flag or (flag == prev_flag and ann and prev_ann and ann >= prev_ann):
            best[key] = s
    cleaned = sorted(best.values(), key=lambda x: x.period)
    annual = [s for s in cleaned if hasattr(s.period, "month") and s.period.month == 12]
    return annual if annual else cleaned


def run(state: ReportState, context: WorkflowContext) -> ReportState:
    logs = state.setdefault("logs", [])
    errors = state.setdefault("errors", [])
    charts: List[dict] = []

    output_dir = Path(context.config.output_dir) / "charts"
    _ensure_output_dir(output_dir)

    ticker = state.get("ticker")

    # Price chart
    price_history = state.get("price_history") or []
    if price_history:
        try:
            dates = [p.get("trade_date") for p in reversed(price_history) if p.get("close") is not None]
            closes = [p.get("close") for p in reversed(price_history) if p.get("close") is not None]
            if dates and closes:
                fig, ax = plt.subplots(figsize=(6, 3))
                ax.plot(dates, closes, label="Close")
                ax.set_title(f"{ticker} Price Trend")
                if len(dates) > 6:
                    ax.set_xticks(dates[:: max(1, len(dates)//6)])
                ax.tick_params(axis="x", rotation=45)
                ax.grid(True, linestyle="--", alpha=0.3)
                ax.legend()
                chart_path = output_dir / f"{ticker}_price.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Price Trend", charts)
            else:
                logs.append("ChartBuilder -> price data empty, skip chart")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder price chart failed: {exc}")

    # Revenue / Net income chart (annual/cleaned)
    dataset: FinancialDataset = state.get("financials")  # type: ignore[assignment]
    if dataset and dataset.income_statements:
        try:
            income_clean = _clean_income_statements(dataset.income_statements)
            periods = [s.period.year if hasattr(s.period, "year") else s.period for s in income_clean]
            revenue = [s.metrics.get("revenue") for s in income_clean]
            net_income = [s.metrics.get("net_income") for s in income_clean]
            if any(revenue) or any(net_income):
                fig, ax = plt.subplots(figsize=(6, 3))
                ax.plot(periods, revenue, marker="o", label="Revenue")
                ax.plot(periods, net_income, marker="o", label="Net Income")
                ax.set_title(f"{ticker} Revenue/Net Income Trend")
                ax.grid(True, linestyle="--", alpha=0.3)
                ax.legend()
                chart_path = output_dir / f"{ticker}_revenue_netincome.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Revenue/Net Income Trend", charts)
            else:
                logs.append("ChartBuilder -> no revenue/net income values, skip chart")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder financial chart failed: {exc}")

    state["charts"] = charts
    return state
