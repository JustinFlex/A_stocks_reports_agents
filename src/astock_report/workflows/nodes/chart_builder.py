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


def _clean_statements(statements):
    """Return one statement per period, preferring revised data; keep annual if available."""
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


def _safe_pct(numerator, denominator):
    try:
        if denominator in (None, 0):
            return None
        return (numerator or 0) / denominator * 100
    except Exception:
        return None


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
    income_clean = _clean_statements(dataset.income_statements) if dataset and dataset.income_statements else []
    balance_clean = _clean_statements(dataset.balance_sheets) if dataset and dataset.balance_sheets else []
    cashflow_clean = _clean_statements(dataset.cash_flows) if dataset and dataset.cash_flows else []

    if income_clean:
        try:
            periods = [s.period.year if hasattr(s.period, "year") else s.period for s in income_clean]
            revenue = [s.metrics.get("revenue") for s in income_clean]
            net_income = [s.metrics.get("net_income") for s in income_clean]
            if any(revenue) or any(net_income):
                fig, ax = plt.subplots(figsize=(6.5, 3.5))
                ax.plot(periods, revenue, marker="o", label="Revenue", color="#38bdf8")
                ax.plot(periods, net_income, marker="o", label="Net Income", color="#a855f7")
                ax.set_title(f"{ticker} Revenue/Net Income Trend")
                ax.grid(True, linestyle="--", alpha=0.3)
                ax.legend()
                chart_path = output_dir / f"{ticker}_revenue_netincome.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Revenue/Net Income Trend", charts)
            else:
                logs.append("ChartBuilder -> no revenue/net income values, skip chart")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder financial chart failed: {exc}")

        # Margin chart: gross/operating/net margin
        try:
            gross_margin = [_safe_pct(s.metrics.get("gross_profit"), s.metrics.get("revenue")) for s in income_clean]
            operating_margin = [_safe_pct(s.metrics.get("operating_income"), s.metrics.get("revenue")) for s in income_clean]
            net_margin = [_safe_pct(s.metrics.get("net_income"), s.metrics.get("revenue")) for s in income_clean]
            if any(gross_margin) or any(operating_margin) or any(net_margin):
                fig, ax = plt.subplots(figsize=(6.5, 3.5))
                if any(gross_margin):
                    ax.plot(periods, gross_margin, marker="o", label="Gross Margin %", color="#22c55e")
                if any(operating_margin):
                    ax.plot(periods, operating_margin, marker="o", label="Operating Margin %", color="#f59e0b")
                if any(net_margin):
                    ax.plot(periods, net_margin, marker="o", label="Net Margin %", color="#f43f5e")
                ax.axhline(0, color="#94a3b8", linewidth=0.8, linestyle="--", alpha=0.6)
                ax.set_title(f"{ticker} Margin Profile")
                ax.set_ylabel("Margin (%)")
                ax.grid(True, linestyle="--", alpha=0.3)
                ax.legend()
                chart_path = output_dir / f"{ticker}_margins.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Margin Profile", charts)
            else:
                logs.append("ChartBuilder -> no margin values, skip chart")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder margin chart failed: {exc}")

        # Margin bridge (latest period waterfall: revenue -> gross -> operating -> net)
        try:
            latest = income_clean[-1]
            rev = latest.metrics.get("revenue") or 0
            gross_profit = latest.metrics.get("gross_profit") or 0
            op_income = latest.metrics.get("operating_income") or 0
            net_income = latest.metrics.get("net_income") or 0
            if rev:
                cogs = rev - gross_profit
                opex = gross_profit - op_income
                below_op = op_income - net_income
                steps = [
                    ("Revenue", rev),
                    ("-COGS", -cogs),
                    ("-Opex", -opex),
                    ("Other", -below_op),
                    ("Net", net_income),
                ]
                cumulative = 0
                x = []
                y = []
                for label, val in steps:
                    x.append(label)
                    y.append(val)
                fig, ax = plt.subplots(figsize=(6.5, 3.5))
                running = 0
                starts = []
                for _, val in steps:
                    starts.append(running)
                    running += val
                colors = ["#38bdf8", "#f43f5e", "#f59e0b", "#a855f7", "#22c55e"]
                for idx, (label, val) in enumerate(steps):
                    ax.bar(label, val, bottom=starts[idx], color=colors[idx % len(colors)], alpha=0.8)
                ax.axhline(0, color="#94a3b8", linewidth=0.8, linestyle="--", alpha=0.6)
                ax.set_title(f"{ticker} Margin Bridge (最新期)")
                ax.set_ylabel("Amount")
                chart_path = output_dir / f"{ticker}_margin_bridge.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Margin Bridge", charts)
            else:
                logs.append("ChartBuilder -> latest revenue empty, skip margin bridge")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder margin bridge failed: {exc}")

    # Cash flow chart (OCF/FCF/CapEx)
    if cashflow_clean:
        try:
            periods_cf = [s.period.year if hasattr(s.period, "year") else s.period for s in cashflow_clean]
            ocf = [s.metrics.get("operating_cash_flow") for s in cashflow_clean]
            fcf = [s.metrics.get("free_cash_flow") for s in cashflow_clean]
            capex = [s.metrics.get("capital_expenditures") for s in cashflow_clean]
            if any(ocf) or any(fcf) or any(capex):
                fig, ax = plt.subplots(figsize=(6.5, 3.5))
                if any(ocf):
                    ax.bar(periods_cf, ocf, label="Operating CF", alpha=0.7, color="#38bdf8")
                if any(capex):
                    ax.bar(periods_cf, [-1 * c if c is not None else 0 for c in capex], label="CapEx (negated)", alpha=0.6, color="#f59e0b")
                if any(fcf):
                    ax.plot(periods_cf, fcf, marker="o", label="Free Cash Flow", color="#22c55e")
                ax.axhline(0, color="#94a3b8", linewidth=0.8, linestyle="--", alpha=0.6)
                ax.set_title(f"{ticker} Cash Flow Mix")
                ax.grid(True, linestyle="--", alpha=0.3)
                ax.legend()
                chart_path = output_dir / f"{ticker}_cashflow.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Cash Flow Mix", charts)
            else:
                logs.append("ChartBuilder -> no cash flow values, skip chart")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder cash flow chart failed: {exc}")

    # Leverage & liquidity chart from balance sheet
    if balance_clean:
        try:
            periods_bs = [s.period.year if hasattr(s.period, "year") else s.period for s in balance_clean]
            debt_to_equity = []
            current_ratio = []
            for s in balance_clean:
                debt = s.metrics.get("total_liabilities")
                equity = s.metrics.get("total_equity")
                ca = s.metrics.get("current_assets")
                cl = s.metrics.get("current_liabilities")
                dte = _safe_pct(debt, equity)
                cr = None
                try:
                    cr = (ca / cl) if ca is not None and cl not in (None, 0) else None
                except Exception:
                    cr = None
                debt_to_equity.append(dte)
                current_ratio.append(cr)
            if any(debt_to_equity) or any(current_ratio):
                fig, ax1 = plt.subplots(figsize=(6.5, 3.5))
                if any(debt_to_equity):
                    ax1.plot(periods_bs, debt_to_equity, marker="o", label="Debt/Equity (%)", color="#f43f5e")
                    ax1.set_ylabel("Debt/Equity (%)")
                ax1.axhline(0, color="#94a3b8", linewidth=0.8, linestyle="--", alpha=0.6)
                ax1.grid(True, linestyle="--", alpha=0.3)

                if any(current_ratio):
                    ax2 = ax1.twinx()
                    ax2.plot(periods_bs, current_ratio, marker="s", label="Current Ratio (x)", color="#38bdf8")
                    ax2.set_ylabel("Current Ratio (x)")
                    # Merge legends
                    lines, labels = ax1.get_legend_handles_labels()
                    lines2, labels2 = ax2.get_legend_handles_labels()
                    ax1.legend(lines + lines2, labels + labels2, loc="upper right")
                else:
                    ax1.legend(loc="upper right")

                ax1.set_title(f"{ticker} Leverage & Liquidity")
                chart_path = output_dir / f"{ticker}_leverage_liquidity.png"
                _maybe_save_chart(fig, chart_path, logs, errors, "Leverage & Liquidity", charts)
            else:
                logs.append("ChartBuilder -> no leverage/liquidity values, skip chart")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(f"ChartBuilder leverage chart failed: {exc}")

    state["charts"] = charts
    return state
